namespace service.messaging.Services
{
    public interface IDoraemonMessageService
    {
        Task SendImageMessageAsync(string imagePath, string message, CancellationToken ct = default);
        Task SendTopicMessageAsync(string topic, string imagePath, string message, CancellationToken ct = default);
    }
}
