package org.itmo.producer.messaging;

import org.itmo.common.RabbitmqNames;
import org.itmo.common.SectionMessage;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

@Service
public class SectionProducer {

    private final RabbitTemplate rabbitTemplate;

    public SectionProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void sendSection(SectionMessage message) {
        rabbitTemplate.convertAndSend(
                RabbitmqNames.TASKS_EXCHANGE,
                RabbitmqNames.ROUTING_SECTION,
                message
        );
    }
}
