using FluentValidation;

using Futurum.Core.Result;
using Futurum.EventApiEndpoint;

namespace Futurum.Kafka.EventApiEndpoint.Sample;

public static class TestBatchRouteEventApiEndpoint
{
    public record EventDto(string Name);
    
    public record Event(string Name);
    
    public class ApiEndpoint : EventApiEndpoint<EventDto, Event>
    {
        public Task<Result> ExecuteAsync(Event @event, CancellationToken cancellationToken) =>
            Result.OkAsync();
    }
    
    public class Mapper : IEventApiEndpointEventMapper<EventDto, Event>
    {
        public Result<Event> Map(EventDto dto) =>
            new Event(dto.Name).ToResultOk();
    }

    public class Validator : AbstractValidator<EventDto>
    {
        public Validator()
        {
            RuleFor(x => x.Name).NotEmpty().WithMessage("must have a value");
        }
    }
}