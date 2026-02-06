using RabbitMQ.Client;
using WebapiMq.Configurations;

namespace WebapiMq.Clients.Connections
{
    public interface IDoraemonMqConnectionFactory
    {
        ConnectionFactory ConnectionFactory { get; }
        RabbitMqSettings RabbitMqSettings { get; }
        ValueTask<IConnection> GetConnectionAsync();
    }
}
