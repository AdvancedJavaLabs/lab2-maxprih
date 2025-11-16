package org.itmo.aggregator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.itmo.aggregator.model.AggregatedJobResult;
import org.itmo.aggregator.model.JobAggregate;
import org.itmo.common.ResultMessage;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

@Service
public class AggregationService {

    private final Map<String, JobAggregate> jobs = new ConcurrentHashMap<>();
    private final Map<String, Integer> jobTopN = new ConcurrentHashMap<>();
    private final Map<String, AggregatedJobResult> completedResults = new ConcurrentHashMap<>();
    private final Deque<String> completionOrder = new ConcurrentLinkedDeque<>();
    private final ObjectMapper objectMapper;
    private final Path resultsDir;
    private final int maxCompletedResults;

    public AggregationService(ObjectMapper objectMapper,
                              @Value("${results.base-path:results}") String resultsDir,
                              @Value("${results.max-completed:200}") int maxCompletedResults) {
        this.objectMapper = objectMapper;
        this.resultsDir = Path.of(resultsDir);
        this.maxCompletedResults = maxCompletedResults;
    }

    public void handleSectionResult(ResultMessage result) {
        String jobId = result.jobId();

        jobTopN.putIfAbsent(jobId, result.topN());

        JobAggregate aggregate = jobs.computeIfAbsent(
                jobId,
                id -> new JobAggregate(id, result.totalSections())
        );
        aggregate.addSectionResult(result);

        if (aggregate.isComplete()) {
            int topN = jobTopN.getOrDefault(jobId, result.topN());
            AggregatedJobResult aggregatedResult = aggregate.toAggregatedResult(topN);
             writeResultToFile(jobId, aggregatedResult);
            storeCompletedResult(jobId, aggregatedResult);
            cleanupJob(jobId);
        }
    }

    private void writeResultToFile(String jobId, AggregatedJobResult aggregatedResult) {
        try {
            Files.createDirectories(resultsDir);
            Path out = resultsDir.resolve(jobId + ".json");
            objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValue(out.toFile(), aggregatedResult);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write result for job " + jobId, e);
        }
    }

    public AggregatedJobResult getResult(String jobId, int topN) {
        AggregatedJobResult completed = completedResults.get(jobId);
        if (completed != null) {
            return completed;
        }

        JobAggregate aggregate = jobs.get(jobId);
        if (aggregate == null) {
            return null;
        }
        return aggregate.toAggregatedResult(topN);
    }

    private void cleanupJob(String jobId) {
        jobs.remove(jobId);
        jobTopN.remove(jobId);
    }

    private void storeCompletedResult(String jobId, AggregatedJobResult result) {
        completedResults.put(jobId, result);
        completionOrder.addLast(jobId);

        while (completionOrder.size() > maxCompletedResults) {
            String evict = completionOrder.pollFirst();
            if (evict != null) {
                completedResults.remove(evict);
            }
        }
    }
}
