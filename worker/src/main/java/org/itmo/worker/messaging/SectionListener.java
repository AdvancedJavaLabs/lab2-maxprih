package org.itmo.worker.messaging;

import org.itmo.common.RabbitmqNames;
import org.itmo.common.ResultMessage;
import org.itmo.common.SectionMessage;
import org.itmo.worker.service.TextProcessingService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

@Component
public class SectionListener {

    private final TextProcessingService textProcessingService;

    public SectionListener(TextProcessingService textProcessingService) {
        this.textProcessingService = textProcessingService;
    }

    @RabbitListener(queues = RabbitmqNames.SECTIONS_QUEUE)
    @SendTo(RabbitmqNames.TASKS_EXCHANGE + "/" + RabbitmqNames.ROUTING_RESULT)
    public ResultMessage handleSection(SectionMessage message) {
        return textProcessingService.processSection(message);
    }
}
