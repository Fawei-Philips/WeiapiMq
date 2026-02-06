using Utils.Ioc;
using service.messaging.Clients.Producer;
using service.messaging.Model;

namespace service.messaging.Services
{
    [Register(ServiceType = typeof(IDoraemonMessageService))]
    public class DoraemonMessageService : IDoraemonMessageService
    {
        private IRabbitMqProducer<DoraemonMessage> _queueModeMqProducer;
        private IRabbitMqProducer<DoraemonTopicMessage> _topicModeMqProducer;

        public DoraemonMessageService(
            [FromKeyedServices(Constants.Ioc_RabbitMq_QueueMode)] IRabbitMqProducer<DoraemonMessage> queueModeMqProducer,
            [FromKeyedServices(Constants.Ioc_RabbitMq_TopicMode)] IRabbitMqProducer<DoraemonTopicMessage> topicModeMqProducer)
        {
            _queueModeMqProducer = queueModeMqProducer;
            _topicModeMqProducer = topicModeMqProducer;
        }

        public async Task SendImageMessageAsync(string imagePath, string message, CancellationToken ct = default)
        {
            await _queueModeMqProducer.ProduceAsync(new DoraemonMessage(imagePath, message), ct);
        }

        public async Task SendTopicMessageAsync(string topic, string imagePath, string message, CancellationToken ct = default)
        {
            await _topicModeMqProducer.ProduceAsync(new DoraemonTopicMessage(topic, imagePath, message), ct);
        }
    }
}
