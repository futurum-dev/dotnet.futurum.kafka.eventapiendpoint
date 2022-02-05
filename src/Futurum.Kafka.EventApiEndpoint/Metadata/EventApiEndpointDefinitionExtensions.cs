namespace Futurum.Kafka.EventApiEndpoint.Metadata;

public static class EventApiEndpointDefinitionExtensions
{
    public static EventApiEndpointDefinition Kafka(this Futurum.EventApiEndpoint.EventApiEndpointDefinition eventApiEndpointDefinition)
    {
        var kafkaEventApiEndpointDefinition = new EventApiEndpointDefinition();
        
        eventApiEndpointDefinition.Add(kafkaEventApiEndpointDefinition);

        return kafkaEventApiEndpointDefinition;
    }
}