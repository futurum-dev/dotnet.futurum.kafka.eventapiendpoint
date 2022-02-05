using Confluent.Kafka;

using Futurum.ApiEndpoint;
using Futurum.Core.Result;

using Serilog;

namespace Futurum.Kafka.EventApiEndpoint;

public interface IEventApiEndpointLogger : Futurum.EventApiEndpoint.IEventApiEndpointLogger, ApiEndpoint.IApiEndpointLogger
{
    void ReachedEndOfTopic(string topic, Partition partition, Offset offset);
    void MessageReceived(string message, TopicPartitionOffset topicPartitionOffset);

    void KafkaCreateConsumerError(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, IMetadataDefinition metadataDefinition, IResultError error);

    void KafkaConfigureSubscriptionError(IMetadataDefinition metadataDefinition, Exception exception);

    void KafkaProcessEventError(IMetadataDefinition metadataDefinition, IResultError error);

    void KafkaCommitError(IMetadataDefinition metadataDefinition, KafkaException exception);
    void KafkaConsumeError(IMetadataDefinition metadataDefinition, ConsumeException consumeException);

    void KafkaConsumerClose(IMetadataDefinition metadataDefinition);
    void KafkaConsumerCloseError(Exception exception);

    void KafkaError(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, IMetadataDefinition metadataDefinition, Error error);

    void KafkaStatistics(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, IMetadataDefinition metadataDefinition, string statistics);

    void KafkaPartitionsLost(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, IMetadataDefinition metadataDefinition, IEnumerable<TopicPartitionOffset> partitions);

    void KafkaPartitionsRevoked(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, IMetadataDefinition metadataDefinition,
                                IEnumerable<TopicPartitionOffset> partitionsIncrementallyRevoked, IEnumerable<TopicPartition> partitionsRemaining);

    void KafkaPartitionsAssigned(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, IMetadataDefinition metadataDefinition,
                                 IEnumerable<TopicPartition> partitionsIncrementallyAssigned, IEnumerable<TopicPartition> allRemaining);
}

public class EventApiEndpointLogger : IEventApiEndpointLogger
{
    private readonly ILogger _logger;

    public EventApiEndpointLogger(ILogger logger)
    {
        _logger = logger;
    }

    public void EventReceived<TEvent>(TEvent @event)
    {
        var eventData = new EventReceivedData<TEvent>(typeof(TEvent), @event);

        _logger.Debug("Kafka EventApiEndpoint event received {@eventData}", eventData);
    }

    public void ReachedEndOfTopic(string topic, Partition partition, Offset offset)
    {
        var eventData = new ReachedEndOfTopicData(topic, partition, offset);

        _logger.Information("Kafka EventApiEndpoint reached end of topic received {@eventData}", eventData);
    }

    public void MessageReceived(string message, TopicPartitionOffset topicPartitionOffset)
    {
        var eventData = new MessageReceivedData(message, topicPartitionOffset);

        _logger.Debug("Kafka EventApiEndpoint message received {@eventData}", eventData);
    }

    public void KafkaCreateConsumerError(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, IMetadataDefinition metadataDefinition, IResultError error)
    {
        var eventData = new CreateConsumerErrorData(connectionConfiguration, metadataDefinition, error.ToErrorString());

        _logger.Error("Kafka EventApiEndpoint CreateConsumer error {@eventData}", eventData);
    }

    public void KafkaConfigureSubscriptionError(IMetadataDefinition metadataDefinition, Exception exception)
    {
        var eventData = new ConfigureSubscriptionErrorData(metadataDefinition);

        _logger.Error(exception, "Kafka EventApiEndpoint ConfigureSubscription error {@eventData}", eventData);
    }

    public void KafkaProcessEventError(IMetadataDefinition metadataDefinition, IResultError error)
    {
        var eventData = new ProcessEventErrorData(metadataDefinition, error.ToErrorString());

        _logger.Error("Kafka EventApiEndpoint ProcessEventError error {@eventData}", eventData);
    }

    public void KafkaCommitError(IMetadataDefinition metadataDefinition, KafkaException exception)
    {
        var eventData = new CommitErrorData(metadataDefinition, exception.Error.Reason);

        _logger.Error(exception, "Kafka EventApiEndpoint Commit error {@eventData}", eventData);
    }

    public void KafkaConsumeError(IMetadataDefinition metadataDefinition, ConsumeException consumeException)
    {
        var eventData = new ConsumeErrorData(metadataDefinition, consumeException.Error.Reason);

        _logger.Error(consumeException, "Kafka EventApiEndpoint Consume error {@eventData}", eventData);
    }

    public void KafkaConsumerClose(IMetadataDefinition metadataDefinition)
    {
        var eventData = new ConsumerCloseData(metadataDefinition);

        _logger.Information("Kafka EventApiEndpoint Consume close {@eventData}", eventData);
    }

    public void KafkaConsumerCloseError(Exception exception)
    {
        _logger.Error(exception, "Kafka EventApiEndpoint StopProcessing error");
    }

    public void KafkaError(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, IMetadataDefinition metadataDefinition, Error error)
    {
        var eventData = new ErrorData(connectionConfiguration, metadataDefinition, error.Reason);

        _logger.Information("Kafka EventApiEndpoint Error {@eventData}", eventData);
    }

    public void KafkaStatistics(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, IMetadataDefinition metadataDefinition, string statistics)
    {
        var eventData = new StatisticsData(connectionConfiguration, metadataDefinition, statistics);

        _logger.Information("Kafka EventApiEndpoint Statistics {@eventData}", eventData);
    }

    public void KafkaPartitionsLost(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, IMetadataDefinition metadataDefinition, IEnumerable<TopicPartitionOffset> partitions)
    {
        var eventData = new PartitionsLostData(connectionConfiguration, metadataDefinition, string.Join(", ", partitions.Select(p => p.Partition.Value)));

        _logger.Information("Kafka EventApiEndpoint PartitionsLost {@eventData}", eventData);
    }

    public void KafkaPartitionsRevoked(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, IMetadataDefinition metadataDefinition,
                                       IEnumerable<TopicPartitionOffset> partitionsIncrementallyRevoked, IEnumerable<TopicPartition> partitionsRemaining)
    {
        var eventData = new PartitionsRevokedData(connectionConfiguration, metadataDefinition, string.Join(", ", partitionsIncrementallyRevoked.Select(p => p.Partition.Value)),
                                                  string.Join(", ", partitionsRemaining.Select(p => p.Partition.Value)));

        _logger.Information("Kafka EventApiEndpoint PartitionsRevoked {@eventData}", eventData);
    }

    public void KafkaPartitionsAssigned(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration, IMetadataDefinition metadataDefinition,
                                        IEnumerable<TopicPartition> partitionsIncrementallyAssigned,
                                        IEnumerable<TopicPartition> allRemaining)
    {
        var eventData = new PartitionsAssignedData(connectionConfiguration, metadataDefinition, string.Join(", ", partitionsIncrementallyAssigned.Select(p => p.Partition.Value)),
                                                   string.Join(", ", allRemaining.Select(p => p.Partition.Value)));

        _logger.Information("Kafka EventApiEndpoint PartitionsAssigned {@eventData}", eventData);
    }

    public void ApiEndpointDebugLog(string apiEndpointDebugLog)
    {
        var eventData = new ApiEndpoints(apiEndpointDebugLog);

        _logger.Debug("WebApiEndpoint endpoints {@eventData}", eventData);
    }

    private readonly record struct EventReceivedData<TEvent>(Type EventType, TEvent Event);

    private readonly record struct ReachedEndOfTopicData(string Topic, Partition Partition, Offset Offset);

    private readonly record struct MessageReceivedData(string Message, TopicPartitionOffset TopicPartitionOffset);

    private readonly record struct CommandReceivedData<TCommand>(Type CommandType, TCommand Command);

    private readonly record struct CreateConsumerErrorData(KafkaEventApiEndpointConnectionConfiguration ConnectionConfiguration, IMetadataDefinition MetadataDefinition, string Error);

    private readonly record struct ConfigureSubscriptionErrorData(IMetadataDefinition MetadataDefinition);

    private readonly record struct ProcessEventErrorData(IMetadataDefinition MetadataDefinition, string Error);

    private readonly record struct ConsumeErrorData(IMetadataDefinition MetadataDefinition, string Reason);

    private readonly record struct CommitErrorData(IMetadataDefinition MetadataDefinition, string Reason);

    private readonly record struct ConsumerCloseData(IMetadataDefinition MetadataDefinition);

    private readonly record struct ErrorData(KafkaEventApiEndpointConnectionConfiguration ConnectionConfiguration, IMetadataDefinition MetadataDefinition, string Reason);

    private readonly record struct StatisticsData(KafkaEventApiEndpointConnectionConfiguration ConnectionConfiguration, IMetadataDefinition MetadataDefinition, string Statistics);

    private readonly record struct PartitionsLostData(KafkaEventApiEndpointConnectionConfiguration ConnectionConfiguration, IMetadataDefinition MetadataDefinition, string Partitions);

    private readonly record struct PartitionsRevokedData(KafkaEventApiEndpointConnectionConfiguration ConnectionConfiguration, IMetadataDefinition MetadataDefinition,
                                                         string PartitionsIncrementallyRevoked, string PartitionsRemaining);

    private readonly record struct PartitionsAssignedData(KafkaEventApiEndpointConnectionConfiguration ConnectionConfiguration, IMetadataDefinition MetadataDefinition,
                                                          string PartitionsIncrementallyAssigned, string AllPartitions);

    private record struct ApiEndpoints(string Log);
}