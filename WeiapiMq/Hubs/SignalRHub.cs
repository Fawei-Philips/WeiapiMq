using System.Text.RegularExpressions;
using Logging;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Options;
using service.messaging.Configurations;

namespace service.messaging.Hubs
{
    public class SignalRHub(IOptions<SignalRSettings> signalRSettings) : Hub
    {

        private readonly SignalRSettings _signalRSettings = signalRSettings.Value;

        /// <summary>
        /// Client connected
        /// </summary>
        public override async Task OnConnectedAsync()
        {
            var connectionId = Context.ConnectionId;
            Log4Logger.Logger.Info($"Client [{connectionId}] connected to SignalR");

            await Groups.AddToGroupAsync(connectionId, _signalRSettings.Group);

            await base.OnConnectedAsync();
        }

        /// <summary>
        /// Client disconnected
        /// </summary>
        public override async Task OnDisconnectedAsync(Exception? exception)
        {
            var connectionId = Context.ConnectionId;
            if (exception != null)
            {
                Log4Logger.Logger.Error($"Client [{connectionId}] disconnected from SignalR", exception);
            }
            else
            {
                Log4Logger.Logger.Info($"Client [{connectionId}] disconnected from SignalR");
            }

            await Groups.RemoveFromGroupAsync(connectionId, _signalRSettings.Group);

            await base.OnDisconnectedAsync(exception);
        }
    }
}
