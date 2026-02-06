using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Channels;
using RabbitMQ.Client.Events;

namespace RabbitMqSimpleClient.Clients
{
    public class QueueModeConsumer
    {

        private readonly ConnectionFactory connectionFactory;
        private const string QueueName = "doraemon.queue";
        private IConnection connection;
        private IChannel channel;
        private AsyncEventingBasicConsumer consumer;

        public QueueModeConsumer()
        {
            connectionFactory = new ConnectionFactory
            {
                HostName = MqConstants.HostName,
                Port = MqConstants.Port,
                UserName = MqConstants.UserName,
                Password = MqConstants.Password,
                VirtualHost = MqConstants.VirtualHost
            };
        }

        public async Task ConsumeAsync()
        {
            connection = await connectionFactory.CreateConnectionAsync();
            channel = await connection.CreateChannelAsync();

            await channel.QueueDeclareAsync(QueueName, true, false, false);
            await channel.BasicQosAsync(0, 1, false);

            consumer = new AsyncEventingBasicConsumer(channel);
            
            consumer.ReceivedAsync += ReceiveQueuedMessageAsync;

            await channel.BasicConsumeAsync(QueueName, autoAck: false, consumer: consumer);

        }

        private async Task ReceiveQueuedMessageAsync(object sender, BasicDeliverEventArgs @event)
        {
            var body = @event.Body.Span;
            var message = Encoding.UTF8.GetString(body);

            Console.WriteLine(message);

            await channel.BasicAckAsync(@event.DeliveryTag, false);
            await Task.Delay(1000);
        }
    }
}
