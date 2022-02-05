using Futurum.ApiEndpoint;
using Futurum.EventApiEndpoint;
using Futurum.Kafka.EventApiEndpoint.Metadata;

namespace Futurum.Kafka.EventApiEndpoint.Sample;

public class ApiEndpointDefinition : IApiEndpointDefinition
{
    public void Configure(ApiEndpointDefinitionBuilder definitionBuilder)
    {
        definitionBuilder.Event()
                         .Kafka()
                         .Event<TestEventApiEndpoint.ApiEndpoint>(builder => builder.Topic("test-topic3").ConsumerGroup("csharp-consumer").CommitPeriod(5))
                         .EnvelopeEvent(builder => builder.FromTopic("test-topic-batch")
                                                          .Route<TestBatchRouteEventApiEndpoint.ApiEndpoint>("test-batch-route"));
    }
}