using Futurum.ApiEndpoint;
using Futurum.ApiEndpoint.DebugLogger;
using Futurum.Core.Result;
using Futurum.EventApiEndpoint;

namespace Futurum.Kafka.EventApiEndpoint.Metadata;

public class EventApiEndpointDefinition : IApiEndpointDefinitionBuilder
{
    private readonly Dictionary<Type, List<Func<MetadataSubscriptionDefinitionBuilder>>> _commandBuilders = new();
    private readonly List<Func<MetadataSubscriptionEnvelopeEventDefinitionBuilder>> _envelopeCommandBuilders = new();

    public Result<Dictionary<Type, List<IMetadataDefinition>>> Build()
    {
        var commandMetadataDefinitions = _commandBuilders.TryToDictionary(keyValuePair => keyValuePair.Key,
                                                                          keyValuePair => keyValuePair.Value.SelectMany(func => func().Build())
                                                                                                      .ToList());

        var envelopeCommandMetadataDefinitions = _envelopeCommandBuilders.SelectMany(x => x().Build())
                                                                         .TryToDictionary(x => x.ApiEndpointType,
                                                                                          x => new List<IMetadataDefinition> { x });
        return Result.CombineAll(commandMetadataDefinitions, envelopeCommandMetadataDefinitions)
                     .Then(x => x.Item1.AsEnumerable().Concat(x.Item2.AsEnumerable())
                                 .TryToDictionary(keyValuePair => keyValuePair.Key, keyValuePair => keyValuePair.Value));
    }

    public ApiEndpointDebugNode Debug()
    {
        var apiEndpointDebugNode = new ApiEndpointDebugNode
        {
            Name = "KAFKA",
            Children = _commandBuilders.SelectMany(keyValuePair => keyValuePair.Value.Select(func => func().Debug()))
                                       .Concat(_envelopeCommandBuilders.Select(keyValuePair => keyValuePair().Debug()))
                                       .ToList()
        };


        return apiEndpointDebugNode;
    }
    
    public EventApiEndpointDefinition Event<TEventApiEndpoint>(Action<MetadataSubscriptionDefinitionBuilder> builderFunc)
        where TEventApiEndpoint : IEventApiEndpoint
    {
        var builder = new MetadataSubscriptionDefinitionBuilder(typeof(TEventApiEndpoint));

        var key = typeof(TEventApiEndpoint);
        var value = () =>
        {
            builderFunc(builder);

            return builder;
        };

        if (_commandBuilders.ContainsKey(key))
        {
            var existingValue = _commandBuilders[key];
            existingValue.Add(value);
        }
        else
        {
            _commandBuilders.Add(key, new List<Func<MetadataSubscriptionDefinitionBuilder>> { value });
        }

        return this;
    }

    public EventApiEndpointDefinition EnvelopeEvent(Action<MetadataSubscriptionEnvelopeEventDefinitionBuilder> builderFunc)
    {
        var builder = new MetadataSubscriptionEnvelopeEventDefinitionBuilder();

        var hasRun = false;
        var value = () =>
        {
            if (!hasRun)
            {
                builderFunc(builder);

                hasRun = true;
            }

            return builder;
        };

        _envelopeCommandBuilders.Add(value);

        return this;
    }
}