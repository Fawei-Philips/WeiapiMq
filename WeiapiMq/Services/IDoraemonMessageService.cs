namespace WebapiMq.Services
{
    public interface IDoraemonMessageService
    {
        Task SendImageMessageAsync(string imagePath, string message);
    }
}
