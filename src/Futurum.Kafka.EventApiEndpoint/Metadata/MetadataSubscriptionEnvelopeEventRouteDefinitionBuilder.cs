using Futurum.ApiEndpoint.DebugLogger;
using Futurum.EventApiEndpoint;
using Futurum.EventApiEndpoint.Metadata;

namespace Futurum.Kafka.EventApiEndpoint.Metadata;

public class MetadataSubscriptionEnvelopeEventRouteDefinitionBuilder
{
    private readonly string _fromTopic;
    private readonly List<MetadataSubscriptionEnvelopeEventDefinition> _metadataSubscriptionEnvelopeEventDefinitions = new();

    public MetadataSubscriptionEnvelopeEventRouteDefinitionBuilder(string fromTopic)
    {
        _fromTopic = fromTopic;
    }

    public MetadataSubscriptionEnvelopeEventRouteDefinitionBuilder Route<TEventApiEndpoint>(string route)
        where TEventApiEndpoint : IEventApiEndpoint
    {
        _metadataSubscriptionEnvelopeEventDefinitions.Add(new MetadataSubscriptionEnvelopeEventDefinition(new MetadataTopic(_fromTopic), new MetadataRoute(route), typeof(TEventApiEndpoint)));

        return this;
    }

    public IEnumerable<MetadataSubscriptionEnvelopeEventDefinition> Build() =>
        _metadataSubscriptionEnvelopeEventDefinitions;

    public IEnumerable<ApiEndpointDebugNode> Debug() =>
        _metadataSubscriptionEnvelopeEventDefinitions.Select(x => new ApiEndpointDebugNode { Name = $"{x.Route} ({x.ApiEndpointType.FullName})" });
}