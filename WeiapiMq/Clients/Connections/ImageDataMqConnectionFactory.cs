using Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using Utils.Ioc;
using Utils.Tasking;
using service.messaging.Configurations;

namespace service.messaging.Clients.Connections
{
    [Register(ServiceType = typeof(IDoraemonMqConnectionFactory), Lifetime = Lifetime.Singleton, Key = Constants.Ioc_RabbitMq_Conn_ImageData)]
    public class ImageDataMqConnectionFactory : IDoraemonMqConnectionFactory
    {
        public ConnectionFactory ConnectionFactory { get; private set; }
        private IConnection? _connection;
        private readonly Task<IConnection> _connectionBuildingTask;

        public RabbitMqSettings RabbitMqSettings { get; }

        public ImageDataMqConnectionFactory(IOptions<RabbitMqSettings> rabbitMqSettingsOption)
        {
            RabbitMqSettings = rabbitMqSettingsOption.Value;
            _connectionBuildingTask = InitAsync();
            _connectionBuildingTask.SafeFireAndForget(OnInitCompleted, OnInitError);
        }

        private void OnInitError(Exception exception)
        {
            Log4Logger.Logger.Error($"RabbitMQ Connection initialized failed.", exception);
        }

        private void OnInitCompleted(IConnection connection)
        {
            _connection = connection;
            Log4Logger.Logger.Info($"RabbitMQ Connection initialized successfully.");
        }

        public async Task<IConnection> InitAsync()
        {
            ConnectionFactory = new ConnectionFactory
            {
                HostName = RabbitMqSettings.HostName,
                Port = RabbitMqSettings.Port,
                UserName = RabbitMqSettings.UserName,
                Password = RabbitMqSettings.Password,
                VirtualHost = RabbitMqSettings.VirtualHost ?? "/"
            };

            var connection = await ConnectionFactory.CreateConnectionAsync();
            return connection;
        }

        public async ValueTask<IConnection> GetConnectionAsync()
        {
            if (_connection != null)
            {
                return _connection;
            }

            return await _connectionBuildingTask;
        }
    }
}
