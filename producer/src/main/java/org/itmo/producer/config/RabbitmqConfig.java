package org.itmo.producer.config;

import org.itmo.common.RabbitmqNames;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitmqConfig {

    @Bean
    public DirectExchange tasksExchange() {
        return new DirectExchange(RabbitmqNames.TASKS_EXCHANGE);
    }

    @Bean
    public Queue sectionsQueue() {
        return new Queue(RabbitmqNames.SECTIONS_QUEUE, true);
    }

    @Bean
    public Queue resultsQueue() {
        return new Queue(RabbitmqNames.RESULTS_QUEUE, true);
    }

    @Bean
    public Binding sectionsBinding(DirectExchange tasksExchange, Queue sectionsQueue) {
        return BindingBuilder.bind(sectionsQueue)
                .to(tasksExchange)
                .with(RabbitmqNames.ROUTING_SECTION);
    }

    @Bean
    public Binding resultsBinding(DirectExchange tasksExchange, Queue resultsQueue) {
        return BindingBuilder.bind(resultsQueue)
                .to(tasksExchange)
                .with(RabbitmqNames.ROUTING_RESULT);
    }

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory,
                                         MessageConverter messageConverter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter);
        return template;
    }
}
