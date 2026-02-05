using Utils.Ioc;
using WebapiMq.Clients.Producer;
using WebapiMq.Model;

namespace WebapiMq.Services
{
    [Register(ServiceType = typeof(IDoraemonMessageService))]
    public class DoraemonMessageService : IDoraemonMessageService
    {
        private IRabbitMqProducer<DoraemonMessage> _queueModeMqProducer;
        public DoraemonMessageService([FromKeyedServices(Constants.Ioc_RabbitMq_QueueMode)]IRabbitMqProducer<DoraemonMessage> queueModeMqProducer)
        {
            _queueModeMqProducer = queueModeMqProducer;
        }

        public async Task SendImageMessageAsync(string imagePath, string message)
        {
            await _queueModeMqProducer.ProduceAsync(new DoraemonMessage(imagePath, message));
        }
    }
}
