using Futurum.ApiEndpoint;
using Futurum.EventApiEndpoint.Metadata;

namespace Futurum.Kafka.EventApiEndpoint.Metadata;

public record KafkaMetadataSubscriptionEventDefinition(MetadataTopic Topic, KafkaMetadataConsumerGroup ConsumerGroup, KafkaMetadataCommitPeriod CommitPeriod) : IMetadataDefinition;

public record KafkaMetadataConsumerGroup(string Value);
public record KafkaMetadataCommitPeriod(long Value);