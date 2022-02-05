using Futurum.ApiEndpoint;
using Futurum.EventApiEndpoint.Metadata;

namespace Futurum.Kafka.EventApiEndpoint.Metadata;

public record MetadataSubscriptionEventDefinition(MetadataTopic Queue) : IMetadataDefinition;