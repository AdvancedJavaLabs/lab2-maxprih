package org.itmo.producer.controller;

import org.itmo.producer.service.JobStarterService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class ProcessController {

    private final JobStarterService jobStarterService;

    public ProcessController(JobStarterService jobStarterService) {
        this.jobStarterService = jobStarterService;
    }

    @PostMapping("/process")
    public ResponseEntity<ProcessResponse> process(@RequestBody ProcessRequest request) {
        String jobId = jobStarterService.startJob(
                request.text(),
                request.topN(),
                request.sentencesPerSection()
        );
        return ResponseEntity.ok(new ProcessResponse(jobId));
    }

    public record ProcessRequest(
            String text,
            int topN,
            int sentencesPerSection
    ) {}

    public record ProcessResponse(String jobId) {}
}
