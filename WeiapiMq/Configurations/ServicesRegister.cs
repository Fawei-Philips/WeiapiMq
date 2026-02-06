using NetUtils.Aspnet.Configurations;
using RabbitMQ.Client;

namespace service.messaging.Configurations
{
    public static class ServicesRegister
    {
        extension(WebApplicationBuilder builder)
        {
            public void RegisterServices()
            {
                builder.RegisterAssembliesAutofac(
                [
                    typeof(Program).Assembly,
                ]);
                builder.RegisterMiddlewares();
                builder.Configurations();
            }

            private void RegisterMiddlewares()
            {
                // Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
                builder.Services.AddOpenApi();
            }

            private void Configurations()
            {
                builder.Services.Configure<RabbitMqSettings>(builder.Configuration.GetSection(Constants.RabbitMqSettings));
                builder.Services.Configure<SignalRSettings>(builder.Configuration.GetSection(Constants.SignalRSettings));
            }
        }
    }
}
