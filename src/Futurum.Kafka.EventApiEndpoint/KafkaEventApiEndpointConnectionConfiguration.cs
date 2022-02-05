using Futurum.Kafka.EventApiEndpoint.Metadata;

namespace Futurum.Kafka.EventApiEndpoint;

/// <summary>
/// Kafka EventApiEndpoint connection
/// </summary>
public record KafkaEventApiEndpointConnectionConfiguration(IEnumerable<string> Brokers, KafkaMetadataConsumerGroup BatchConsumerGroup, KafkaMetadataCommitPeriod BatchCommitPeriod);