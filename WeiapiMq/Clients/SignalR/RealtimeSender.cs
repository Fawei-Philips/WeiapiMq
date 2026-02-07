using Logging;
using Microsoft.AspNetCore.SignalR;
using service.messaging.Hubs;
using service.messaging.Model;

namespace service.messaging.Clients.SignalR
{
    public class RealtimeSender(IHubContext<SignalRHub> hubContext) : IRealtimeSender
    {

        private readonly IHubContext<SignalRHub> _hubContext = hubContext;

        public async Task SendToAllClientsAsync(DoraemonMessage message, CancellationToken cancellationToken = default)
        {
            try
            {
                Log4Logger.Logger.Info($"Start sending SignalR message to [all clients], message：[{message.ImagePath}, {message.Message}]");

                await _hubContext.Clients.All   
                    .SendAsync("ReceiveRealTimeMessage", message, cancellationToken);

                Log4Logger.Logger.Info($"Successfully sent SignalR message to [all clients], message：[{message.ImagePath}, {message.Message}]");
            }
            catch (OperationCanceledException)
            {

                Log4Logger.Logger.Warn($"A SignalR message has been cancelled. [all clients], message：[{message.ImagePath}, {message.Message}]");
                throw;
            }
            catch (Exception ex)
            {
                Log4Logger.Logger.Error($"Failed to send message to [all clients], ", ex);
                throw new InvalidOperationException($"Failed to send message to [all clients]. Error：{ex.Message}", ex);
            }
        }

        public async Task SendToGroupAsync(string groupName, DoraemonMessage message, CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(groupName);

            try
            {
                Log4Logger.Logger.Info($"Start sending SignalR message to [{groupName}], message：[{message.ImagePath}, {message.Message}]");

                await _hubContext.Clients.Group(groupName)
                    .SendAsync("ReceiveRealTimeMessage", message, cancellationToken);

                Log4Logger.Logger.Info($"Successfully sent SignalR message to [{groupName}], message：[{message.ImagePath}, {message.Message}]");
            }
            catch (OperationCanceledException)
            {

                Log4Logger.Logger.Warn($"A SignalR message has been cancelled. [{groupName}], message：[{message.ImagePath}, {message.Message}]");
                throw;
            }
            catch (Exception ex)
            {
                Log4Logger.Logger.Error($"Failed to send message to group [{groupName}], ", ex);
                throw new InvalidOperationException($"Failed to send message to group [{groupName}]. Error：{ex.Message}", ex);
            }
        }
    }
}
