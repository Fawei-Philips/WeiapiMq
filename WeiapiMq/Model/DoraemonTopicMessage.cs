using service.messaging.Model;

namespace service.messaging.Model
{
    public record DoraemonTopicMessage(string Topic, string ImagePath, string Message) : ITopicMessage;
}
