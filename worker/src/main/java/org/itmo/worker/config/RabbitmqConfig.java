package org.itmo.worker.config;

import org.itmo.common.RabbitmqNames;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
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
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory,
            MessageConverter messageConverter,
            @Value("${listener.concurrency:1}") int concurrency,
            @Value("${listener.max-concurrency:4}") int maxConcurrency,
            @Value("${listener.prefetch:1}") int prefetch
    ) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setMessageConverter(messageConverter);
        factory.setConcurrentConsumers(concurrency);
        factory.setMaxConcurrentConsumers(maxConcurrency);
        factory.setPrefetchCount(prefetch);
        return factory;
    }
}
