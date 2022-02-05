using Futurum.Kafka.EventApiEndpoint;
using Futurum.Kafka.EventApiEndpoint.Metadata;
using Futurum.Kafka.EventApiEndpoint.Sample;
using Futurum.Microsoft.Extensions.DependencyInjection;

using Serilog;

try
{
    var configurationBuilder = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory())
                                                         .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                                                         .AddEnvironmentVariables();

    Log.Logger = new LoggerConfiguration().ReadFrom.Configuration(configurationBuilder.Build())
                                          .Enrich.FromLogContext()
                                          .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss.fff} {Level:u3}] {Message:lj}{NewLine}{Exception}")
                                          .CreateBootstrapLogger();

    Log.Information("Application starting up");

    var builder = Host.CreateDefaultBuilder(args);

    builder.UseSerilog((hostBuilderContext, loggerConfiguration) =>
                           loggerConfiguration.WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss.fff} {Level:u3}] {Message:lj}{NewLine}{Exception}")
                                              .ReadFrom.Configuration(hostBuilderContext.Configuration));

    builder.ConfigureServices(serviceCollection =>
    {
        serviceCollection.RegisterModule(new KafkaEventApiEndpointModule(new(new[] { "localhost:9092" }, new KafkaMetadataConsumerGroup("consumer-group"), new KafkaMetadataCommitPeriod(5)),
                                                                         typeof(AssemblyHook).Assembly));

        serviceCollection.RegisterModule<ApplicationModule>();
    });

    builder.ConfigureServices(services => { services.AddHostedService<Worker>(); });

    var host = builder.Build();

    await host.RunAsync();
}
catch (Exception exception)
{
    Log.Fatal(exception, "Application start-up failed");
}
finally
{
    Log.Information("Application shut down complete");
    Log.CloseAndFlush();
}