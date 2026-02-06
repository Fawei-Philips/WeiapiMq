using Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System.Text;
using System.Text.Json;
using Utils.Ioc;
using Utils.Tasking;
using service.messaging.Clients.Connections;
using service.messaging.Model;

namespace service.messaging.Clients.Producer
{
    [Register(ServiceType = typeof(IRabbitMqProducer<DoraemonTopicMessage>), Key = Constants.Ioc_RabbitMq_TopicMode, Lifetime = Lifetime.Singleton)]
    public class MqProducerTopicMode : IRabbitMqProducer<DoraemonTopicMessage>
    {

        private readonly IDoraemonMqConnectionFactory _connectionFactory;
        private IChannel _channel;
        private readonly Task _connectionBuildingTask;

        #region Init

        public MqProducerTopicMode([FromKeyedServices(Constants.Ioc_RabbitMq_Conn_ImageData)] IDoraemonMqConnectionFactory connectionFactory)
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

            await _channel.ExchangeDeclareAsync(exchange: _connectionFactory.RabbitMqSettings.ExchangeName, type: ExchangeType.Topic, durable:true);
        }

        #endregion Init

        public async Task ProduceAsync(DoraemonTopicMessage messagePayload, CancellationToken ct = default)
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
                await _channel.BasicPublishAsync(exchange: _connectionFactory.RabbitMqSettings.ExchangeName,
                    routingKey: messagePayload.Topic,
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
