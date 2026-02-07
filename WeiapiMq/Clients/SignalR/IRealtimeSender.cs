using service.messaging.Model;

namespace service.messaging.Clients.SignalR
{
    public interface IRealtimeSender
    {
        /// <summary>
        /// Broadcast message to all connected clients
        /// </summary>
        /// <param name="message">Message object</param>
        /// <param name="cancellationToken">Cancellation token</param>
        Task SendToAllClientsAsync(DoraemonMessage message, CancellationToken cancellationToken = default);

        /// <summary>
        /// Send message to specific group of clients
        /// </summary>
        /// <param name="groupName">Group/Topic name</param>
        /// <param name="message">Message object</param>
        /// <param name="cancellationToken">Cancellation token</param>
        Task SendToGroupAsync(string groupName, DoraemonMessage message, CancellationToken cancellationToken = default);
    }
}
