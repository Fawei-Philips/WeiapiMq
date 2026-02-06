// See https://aka.ms/new-console-template for more information

using RabbitMqSimpleClient.Clients;

//await new QueueModeConsumer().ConsumeAsync();
await new TopicModeConsumer().ConsumeAsync();


Console.ReadLine();
