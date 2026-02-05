using RabbitMQ.Client;

namespace WebapiMq.Clients.Connections
{
    public interface IDoraemonMqConnectionFactory
    {
        ConnectionFactory ConnectionFactory { get; }
        ValueTask<IConnection> GetConnectionAsync();
    }
}
