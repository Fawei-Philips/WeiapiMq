using RabbitMQ.Client;
using service.messaging.Configurations;

namespace service.messaging.Clients.Connections
{
    public interface IDoraemonMqConnectionFactory
    {
        ConnectionFactory ConnectionFactory { get; }
        RabbitMqSettings RabbitMqSettings { get; }
        ValueTask<IConnection> GetConnectionAsync();
    }
}
