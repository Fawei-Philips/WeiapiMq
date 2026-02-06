using System.Text.RegularExpressions;
using Microsoft.AspNetCore.SignalR;

namespace service.messaging.Hubs
{
    public class RealtimeHub : Hub
    {
        /// <summary>
        /// Client connected
        /// </summary>
        public override async Task OnConnectedAsync()
        {
            var connectionId = Context.ConnectionId;
            _logger.LogInformation("客户端 {ConnectionId} 已连接SignalR", connectionId);

            // 可选：将连接ID加入默认组（方便按组推送）
            await Groups.AddToGroupAsync(connectionId, "DefaultGroup");

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
                _logger.LogError(exception, "客户端 {ConnectionId} 异常断开", connectionId);
            }
            else
            {
                _logger.LogInformation("客户端 {ConnectionId} 正常断开", connectionId);
            }

            // 可选：移除分组
            await Groups.RemoveFromGroupAsync(connectionId, "DefaultGroup");

            await base.OnDisconnectedAsync(exception);
        }
    }
}
