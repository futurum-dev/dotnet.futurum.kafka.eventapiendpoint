using System.Reflection;

using Futurum.EventApiEndpoint;
using Futurum.Microsoft.Extensions.DependencyInjection;

using Microsoft.Extensions.DependencyInjection;

namespace Futurum.Kafka.EventApiEndpoint;

public class KafkaEventApiEndpointModule : IModule
{
    private readonly EventApiEndpointConfiguration _configuration;
    private readonly KafkaEventApiEndpointConnectionConfiguration _connectionConfiguration;
    private readonly Assembly[] _assemblies;

    public KafkaEventApiEndpointModule(EventApiEndpointConfiguration configuration,
                                       KafkaEventApiEndpointConnectionConfiguration connectionConfiguration,
                                       params Assembly[] assemblies)
    {
        _configuration = configuration;
        _connectionConfiguration = connectionConfiguration;
        _assemblies = assemblies;
    }

    public KafkaEventApiEndpointModule(KafkaEventApiEndpointConnectionConfiguration connectionConfiguration,
                                       params Assembly[] assemblies)
        : this(EventApiEndpointConfiguration.Default, connectionConfiguration, assemblies)
    {
    }

    public void Load(IServiceCollection services)
    {
        services.RegisterModule(new EventApiEndpointModule(_configuration, _assemblies));

        services.AddSingleton(_connectionConfiguration);

        services.AddSingleton<IEventApiEndpointLogger, EventApiEndpointLogger>();
        services.AddSingleton<Futurum.EventApiEndpoint.IEventApiEndpointLogger, EventApiEndpointLogger>();
        services.AddSingleton<ApiEndpoint.IApiEndpointLogger, EventApiEndpointLogger>();

        services.AddSingleton<IKafkaEventApiEndpointWorkerService, KafkaEventApiEndpointWorkerService>();
    }
}