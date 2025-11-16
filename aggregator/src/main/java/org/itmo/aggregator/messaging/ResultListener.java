package org.itmo.aggregator.messaging;

import org.itmo.aggregator.service.AggregationService;
import org.itmo.common.RabbitmqNames;
import org.itmo.common.ResultMessage;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class ResultListener {

    private final AggregationService aggregationService;

    public ResultListener(AggregationService aggregationService) {
        this.aggregationService = aggregationService;
    }

    @RabbitListener(queues = RabbitmqNames.RESULTS_QUEUE)
    public void handleResult(ResultMessage result) {
        aggregationService.handleSectionResult(result);
    }
}
