using Futurum.ApiEndpoint;
using Futurum.ApiEndpoint.DebugLogger;
using Futurum.EventApiEndpoint.Metadata;

namespace Futurum.Kafka.EventApiEndpoint.Metadata;

public class MetadataSubscriptionDefinitionBuilder : IMetadataEventSubscriptionDefinitionBuilder
{
    private readonly Type _apiEndpointType;
    private string _topic;
    private string _consumerGroup;
    private long _commitPeriod;

    public MetadataSubscriptionDefinitionBuilder(Type apiEndpointType)
    {
        _apiEndpointType = apiEndpointType;
    }

    public MetadataSubscriptionDefinitionBuilder Topic(string topic)
    {
        _topic = topic;

        return this;
    }

    public MetadataSubscriptionDefinitionBuilder ConsumerGroup(string consumerGroup)
    {
        _consumerGroup = consumerGroup;

        return this;
    }

    public MetadataSubscriptionDefinitionBuilder CommitPeriod(long commitPeriod)
    {
        _commitPeriod = commitPeriod;

        return this;
    }

    public IEnumerable<IMetadataDefinition> Build()
    {
        yield return new KafkaMetadataSubscriptionEventDefinition(new MetadataTopic(_topic), new KafkaMetadataConsumerGroup(_consumerGroup), new KafkaMetadataCommitPeriod(_commitPeriod));
    }

    public ApiEndpointDebugNode Debug() =>
        new()
        {
            Name = $"{_topic} ({_apiEndpointType.FullName})"
        };
}