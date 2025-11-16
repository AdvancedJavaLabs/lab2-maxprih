package org.itmo.aggregator.controller;

import org.itmo.aggregator.model.AggregatedJobResult;
import org.itmo.aggregator.service.AggregationService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final AggregationService aggregationService;

    public JobController(AggregationService aggregationService) {
        this.aggregationService = aggregationService;
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<AggregatedJobResult> getJob(@PathVariable String jobId,
                                                      @RequestParam(defaultValue = "10") int topN) {
        AggregatedJobResult result = aggregationService.getResult(jobId, topN);
        if (result == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(result);
    }
}
