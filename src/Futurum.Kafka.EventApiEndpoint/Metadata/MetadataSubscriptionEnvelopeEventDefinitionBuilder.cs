using Futurum.ApiEndpoint.DebugLogger;
using Futurum.EventApiEndpoint.Metadata;

namespace Futurum.Kafka.EventApiEndpoint.Metadata;

public class MetadataSubscriptionEnvelopeEventDefinitionBuilder
{
    private string? _fromTopic;
    private readonly List<MetadataSubscriptionEnvelopeEventRouteDefinitionBuilder> _routes = new();

    public MetadataSubscriptionEnvelopeEventRouteDefinitionBuilder FromTopic(string fromTopic)
    {
        _fromTopic = fromTopic;

        var envelopeEventRouteDefinitionBuilder = new MetadataSubscriptionEnvelopeEventRouteDefinitionBuilder(fromTopic);

        _routes.Add(envelopeEventRouteDefinitionBuilder);

        return envelopeEventRouteDefinitionBuilder;
    }

    public IEnumerable<MetadataSubscriptionEnvelopeEventDefinition> Build() =>
        _routes.SelectMany(x => x.Build());

    public ApiEndpointDebugNode Debug() =>
        new()
        {
            Name = $"{_fromTopic} (ENVELOPE)",
            Children = _routes.SelectMany(x => x.Debug()).ToList()
        };
}