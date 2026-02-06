using Asp.Versioning;
using Microsoft.AspNetCore.Mvc;
using service.messaging.Model;
using service.messaging.Services;

namespace service.messaging.Controllers.RabbitMq
{
    [ApiController]
    [Route("api/[controller]")]
    [ApiVersion(1.0)]
    public class RabbitMqMessagingController : ControllerBase
    {
        private readonly ILogger<RabbitMqMessagingController> _logger;
        private IDoraemonMessageService _messageService;

        public RabbitMqMessagingController(ILogger<RabbitMqMessagingController> logger, IDoraemonMessageService messageService)
        {
            _logger = logger;
            _messageService = messageService;
        }

        [HttpPost]
        [Route("DoraemonMessage")]
        public async Task<IActionResult> SendMessage([FromBody] DoraemonMessage message)
        {
            await _messageService.SendImageMessageAsync(message.ImagePath, message.Message);
            return Ok();
        }

        [HttpPost]
        [Route("DoraemonTopicMessage")]
        public async Task<IActionResult> SendTopicMessage([FromBody] DoraemonTopicMessage message)
        {
            await _messageService.SendTopicMessageAsync(message.Topic, message.ImagePath, message.Message);
            return Ok();
        }
    }
}
