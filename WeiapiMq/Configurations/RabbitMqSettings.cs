namespace WebapiMq.Configurations
{
    public record RabbitMqSettings
    {
        public required string HostName { get; init; }
        public required int Port { get; init; }
        public required string UserName { get; init; }
        public required string Password { get; init; }
        public string? VirtualHost { get; init; }
    }
}
