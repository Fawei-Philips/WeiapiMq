using Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Utils.Ioc;
using Utils.Tasking;
using WebapiMq.Clients.Connections;
using WebapiMq.Configurations;
using WebapiMq.Model;

namespace WebapiMq.Clients.Producer
{
    [Register(ServiceType = typeof(IRabbitMqProducer<DoraemonMessage>), Key = Constants.Ioc_RabbitMq_QueueMode, Lifetime = Lifetime.Singleton)]
    public class MqProducerQueueMode : IRabbitMqProducer<DoraemonMessage>
    {
        private readonly IDoraemonMqConnectionFactory _connectionFactory;
        private IChannel _channel;
        private readonly Task _connectionBuildingTask;

        private const string QueueName = "doraemon.queue";

        #region Init

        public MqProducerQueueMode([FromKeyedServices(Constants.Ioc_RabbitMq_Conn_ImageData)] IDoraemonMqConnectionFactory connectionFactory)
        {
            _connectionFactory = connectionFactory;

            _connectionBuildingTask = BuildConnectionAsync();
            _connectionBuildingTask.SafeFireAndForget(OnCompleted, OnError);
        }

        private void OnError(Exception exception)
        {
            Log4Logger.Logger.Error($"RabbitMQ QueueMode Producer initialized failed.", exception);
        }

        private void OnCompleted()
        {
            Log4Logger.Logger.Info($"RabbitMQ QueueMode Producer initialized successfully.");
        }

        private async Task BuildConnectionAsync()
        {
            var connection = await _connectionFactory.GetConnectionAsync();
            _channel = await connection.CreateChannelAsync();

            await _channel.QueueDeclareAsync(QueueName, true, false, false);
        }

        #endregion Init

        public async Task ProduceAsync(DoraemonMessage messagePayload, CancellationToken ct = default)
        {
            ArgumentNullException.ThrowIfNull(messagePayload);

            await _connectionBuildingTask;

            var properties = new BasicProperties
            {
                Persistent = true
            };

            try
            {
                var jsonMsg = JsonSerializer.Serialize(messagePayload);
                byte[] msgBody = Encoding.UTF8.GetBytes(jsonMsg);
                await _channel.BasicPublishAsync(exchange: string.Empty,
                    routingKey: QueueName,
                    mandatory: true,
                    basicProperties: properties,
                    body: msgBody,
                    cancellationToken: ct
                );
            }
            catch (PublishException pex)
            {
                Log4Logger.Logger.Error(pex);
                throw;
            }
            catch (Exception e)
            {
                Log4Logger.Logger.Error(e);
                throw;
            }
        }
    }
}
