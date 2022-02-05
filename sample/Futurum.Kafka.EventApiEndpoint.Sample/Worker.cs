namespace Futurum.Kafka.EventApiEndpoint.Sample;

public class Worker : BackgroundService
{
    private readonly IKafkaEventApiEndpointWorkerService _workerService;

    public Worker(IKafkaEventApiEndpointWorkerService workerService)
    {
        _workerService = workerService;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _workerService.Execute(stoppingToken);

        return Task.CompletedTask;
    }

    public override Task StopAsync(CancellationToken cancellationToken)
    {
        _workerService.Stop();

        return Task.CompletedTask;
    }
}