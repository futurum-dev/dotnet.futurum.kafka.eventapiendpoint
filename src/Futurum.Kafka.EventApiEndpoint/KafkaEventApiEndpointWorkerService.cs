using Confluent.Kafka;

using Futurum.Core.Linq;
using Futurum.Core.Result;
using Futurum.EventApiEndpoint;
using Futurum.EventApiEndpoint.Metadata;
using Futurum.Kafka.EventApiEndpoint.Metadata;
using Futurum.Microsoft.Extensions.DependencyInjection;

using Microsoft.Extensions.DependencyInjection;

namespace Futurum.Kafka.EventApiEndpoint;

public interface IKafkaEventApiEndpointWorkerService
{
    void Execute(CancellationToken cancellationToken);

    void Stop();
}

public class KafkaEventApiEndpointWorkerService : IKafkaEventApiEndpointWorkerService
{
    private readonly IEventApiEndpointLogger _logger;
    private readonly KafkaEventApiEndpointConnectionConfiguration _connectionConfiguration;
    private readonly IServiceProvider _serviceProvider;
    private readonly IEventApiEndpointMetadataCache _metadataCache;

    private readonly List<IConsumer<Ignore, string>> _consumers = new();

    public KafkaEventApiEndpointWorkerService(IEventApiEndpointLogger logger,
                                              KafkaEventApiEndpointConnectionConfiguration connectionConfiguration,
                                              IServiceProvider serviceProvider,
                                              IEventApiEndpointMetadataCache metadataCache)
    {
        _logger = logger;
        _connectionConfiguration = connectionConfiguration;
        _serviceProvider = serviceProvider;
        _metadataCache = metadataCache;
    }

    public void Execute(CancellationToken cancellationToken)
    {
        ConfigureCommands(_connectionConfiguration, cancellationToken);

        ConfigureBatchCommands(_connectionConfiguration, cancellationToken);
    }

    private void ConfigureCommands(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, CancellationToken cancellationToken)
    {
        var metadataEventDefinitions = _metadataCache.GetMetadataEventDefinitions();

        foreach (var (metadataSubscriptionDefinition, metadataTypeDefinition) in metadataEventDefinitions)
        {
            if (metadataSubscriptionDefinition is KafkaMetadataSubscriptionEventDefinition kafkaMetadataSubscriptionCommandDefinition)
            {
                CreateConsumer(connectionConfiguration, kafkaMetadataSubscriptionCommandDefinition)
                    .DoAsync(consumer =>
                    {
                        _consumers.Add(consumer);

                        return ConfigureSubscription(kafkaMetadataSubscriptionCommandDefinition, metadataTypeDefinition.EventApiEndpointExecutorServiceType, consumer, cancellationToken);
                    });
            }
        }
    }

    private void ConfigureBatchCommands(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, CancellationToken cancellationToken)
    {
        var metadataEnvelopeEventDefinitions = _metadataCache.GetMetadataEnvelopeEventDefinitions();

        var metadataSubscriptionEventDefinitions = metadataEnvelopeEventDefinitions.Select(x => x.MetadataSubscriptionEventDefinition)
                                                                                   .Select(x => x.FromTopic)
                                                                                   .Distinct()
                                                                                   .Select(topic => new KafkaMetadataSubscriptionEventDefinition(
                                                                                               topic, connectionConfiguration.BatchConsumerGroup, connectionConfiguration.BatchCommitPeriod));

        foreach (var envelopeMetadataSubscriptionCommandDefinition in metadataSubscriptionEventDefinitions)
        {
            CreateConsumer(connectionConfiguration, envelopeMetadataSubscriptionCommandDefinition)
                .DoAsync(consumer =>
                {
                    _consumers.Add(consumer);

                    var apiEndpointExecutorServiceType = typeof(EventApiEndpointExecutorService<,>).MakeGenericType(typeof(Batch.EventDto), typeof(Batch.Event));

                    return ConfigureSubscription(envelopeMetadataSubscriptionCommandDefinition, apiEndpointExecutorServiceType, consumer, cancellationToken);
                });
        }
    }

    public void Stop()
    {
        foreach (var consumer in _consumers)
        {
            try
            {
                consumer.Close();
            }
            catch (Exception exception)
            {
                _logger.KafkaConsumerCloseError(exception);
            }
        }
    }

    private Result<IConsumer<Ignore, string>> CreateConsumer(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration,
                                                             KafkaMetadataSubscriptionEventDefinition metadataSubscriptionEventDefinition)
    {
        ConsumerConfig CreateConsumerConfig()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = connectionConfiguration.Brokers.StringJoin(","),
                GroupId = metadataSubscriptionEventDefinition.ConsumerGroup.Value,
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true,
                // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
                // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
                PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
            };
            return config;
        }

        IConsumer<Ignore, string> Execute()
        {
            var config = CreateConsumerConfig();

            return new ConsumerBuilder<Ignore, string>(config)
                   // Note: All handlers are called on the main .Consume thread.
                   .SetErrorHandler((_, e) => OnError(connectionConfiguration, metadataSubscriptionEventDefinition, e))
                   .SetStatisticsHandler((_, json) => OnStatistics(connectionConfiguration, metadataSubscriptionEventDefinition, json))
                   .SetPartitionsAssignedHandler((consumer, partitions) => OnPartitionsAssigned(connectionConfiguration, metadataSubscriptionEventDefinition, consumer, partitions))
                   .SetPartitionsRevokedHandler((consumer, partitions) => OnPartitionsRevoked(connectionConfiguration, metadataSubscriptionEventDefinition, consumer, partitions))
                   .SetPartitionsLostHandler((c, partitions) => OnPartitionsLost(connectionConfiguration, metadataSubscriptionEventDefinition, partitions))
                   .Build();
        }

        return Result.Try(Execute, () => $"Failed to create Kafka Consumer")
                     .DoWhenFailure(error => _logger.KafkaCreateConsumerError(connectionConfiguration, metadataSubscriptionEventDefinition, error));
    }

    private async Task ConfigureSubscription(KafkaMetadataSubscriptionEventDefinition metadataSubscriptionEventDefinition, Type apiEndpointExecutorServiceType, IConsumer<Ignore, string> consumer,
                                             CancellationToken cancellationToken)
    {
        try
        {
            consumer.Subscribe(new[] { metadataSubscriptionEventDefinition.Topic.Value });

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(cancellationToken);

                        if (consumeResult.IsPartitionEOF)
                        {
                            _logger.ReachedEndOfTopic(consumeResult.Topic, consumeResult.Partition, consumeResult.Offset);

                            continue;
                        }

                        await ProcessMessage(metadataSubscriptionEventDefinition, apiEndpointExecutorServiceType, consumeResult, cancellationToken);

                        CommitBasedOnOffset(metadataSubscriptionEventDefinition, consumer, consumeResult);
                    }
                    catch (ConsumeException consumeException)
                    {
                        _logger.KafkaConsumeError(metadataSubscriptionEventDefinition, consumeException);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.KafkaConsumerClose(metadataSubscriptionEventDefinition);

                consumer.Close();
            }
        }
        catch (Exception exception)
        {
            _logger.KafkaConfigureSubscriptionError(metadataSubscriptionEventDefinition, exception);
        }
    }

    private void CommitBasedOnOffset(KafkaMetadataSubscriptionEventDefinition metadataSubscriptionEventDefinition, IConsumer<Ignore, string> consumer, ConsumeResult<Ignore, string> consumeResult)
    {
        if (consumeResult.Offset % metadataSubscriptionEventDefinition.CommitPeriod.Value != 0) return;

        // The Commit method sends a "commit offsets" request to the Kafka
        // cluster and synchronously waits for the response. This is very
        // slow compared to the rate at which the consumer is capable of
        // consuming messages. A high performance application will typically
        // commit offsets relatively infrequently and be designed handle
        // duplicate messages in the event of failure.
        try
        {
            consumer.Commit(consumeResult);
        }
        catch (KafkaException kafkaException)
        {
            _logger.KafkaCommitError(metadataSubscriptionEventDefinition, kafkaException);
        }
    }

    private async Task ProcessMessage(KafkaMetadataSubscriptionEventDefinition metadataSubscriptionEventDefinition, Type apiEndpointExecutorServiceType,
                                      ConsumeResult<Ignore, string> consumeResult, CancellationToken cancellationToken)
    {
        await using var scope = _serviceProvider.CreateAsyncScope();

        var message = consumeResult.Message.Value;

        _logger.MessageReceived(message, consumeResult.TopicPartitionOffset);

        await scope.ServiceProvider.TryGetService<IEventApiEndpointExecutorService>(apiEndpointExecutorServiceType)
                   .ThenAsync(apiEndpointExecutorService => apiEndpointExecutorService.ExecuteAsync(metadataSubscriptionEventDefinition, message, cancellationToken))
                   .DoWhenFailureAsync(error => _logger.KafkaProcessEventError(metadataSubscriptionEventDefinition, error))
                   .UnwrapAsync();
    }

    private void OnError(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, KafkaMetadataSubscriptionEventDefinition metadataSubscriptionEventDefinition, Error error)
    {
        _logger.KafkaError(connectionConfiguration, metadataSubscriptionEventDefinition, error);
    }

    private void OnStatistics(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, KafkaMetadataSubscriptionEventDefinition metadataSubscriptionEventDefinition, string json)
    {
        _logger.KafkaStatistics(connectionConfiguration, metadataSubscriptionEventDefinition, json);
    }

    private void OnPartitionsAssigned(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, KafkaMetadataSubscriptionEventDefinition metadataSubscriptionEventDefinition,
                                      IConsumer<Ignore, string> c, List<TopicPartition> partitions)
    {
        // Since a cooperative assignor (CooperativeSticky) has been configured, the
        // partition assignment is incremental (adds partitions to any existing assignment).

        var partitionsIncrementallyAssigned = partitions;
        var allPartitions = c.Assignment.Concat(partitionsIncrementallyAssigned);

        _logger.KafkaPartitionsAssigned(connectionConfiguration, metadataSubscriptionEventDefinition, partitionsIncrementallyAssigned, allPartitions);

        // Possibly manually specify start offsets by returning a list of topic/partition/offsets
        // to assign to, e.g.:
        // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
    }

    private void OnPartitionsRevoked(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, KafkaMetadataSubscriptionEventDefinition metadataSubscriptionEventDefinition,
                                     IConsumer<Ignore, string> c, List<TopicPartitionOffset> partitions)
    {
        // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
        // assignment is incremental (may remove only some partitions of the current assignment).
        var partitionsIncrementallyRevoked = partitions;
        var partitionsRemaining = (IEnumerable<TopicPartition>?)c.Assignment.Where(atp => partitions.All(rtp => rtp.TopicPartition != atp));

        _logger.KafkaPartitionsRevoked(connectionConfiguration, metadataSubscriptionEventDefinition, partitionsIncrementallyRevoked, partitionsRemaining);
    }

    private void OnPartitionsLost(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, KafkaMetadataSubscriptionEventDefinition metadataSubscriptionEventDefinition,
                                  IEnumerable<TopicPartitionOffset> partitions)
    {
        // The lost partitions handler is called when the consumer detects that it has lost ownership
        // of its assignment (fallen out of the group).
        _logger.KafkaPartitionsLost(connectionConfiguration, metadataSubscriptionEventDefinition, partitions);
    }
}