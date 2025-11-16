package org.itmo.aggregator.model;

import org.itmo.common.ResultMessage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class JobAggregate {

    private final String jobId;
    private final int expectedSections;

    private final Map<Integer, ResultMessage> sections = new ConcurrentHashMap<>();
    private final Map<String, Long> globalFreq = new HashMap<>();
    private long totalWordCount = 0;
    private double sentimentNetSum = 0.0;

    public JobAggregate(String jobId, int expectedSections) {
        this.jobId = jobId;
        this.expectedSections = expectedSections;
    }

    public synchronized void addSectionResult(ResultMessage result) {
        if (sections.putIfAbsent(result.sectionId(), result) == null) {
            totalWordCount += result.wordCount();
            mergeFreq(result.wordFreq());
            sentimentNetSum += result.sentimentScore() * result.wordCount();
        }
    }

    private void mergeFreq(Map<String, Long> localFreq) {
        for (Map.Entry<String, Long> e : localFreq.entrySet()) {
            globalFreq.merge(e.getKey(), e.getValue(), Long::sum);
        }
    }

    public boolean isComplete() {
        return sections.size() >= expectedSections;
    }

    public AggregatedJobResult toAggregatedResult(int topN) {
        List<AggregatedJobResult.WordCount> globalTop =
                computeTopN(globalFreq, topN);

        double avgSentiment = totalWordCount == 0
                ? 0.0
                : sentimentNetSum / totalWordCount;

        StringBuilder anonymized = new StringBuilder();
        List<String> allSentences = new ArrayList<>();

        sections.values().stream()
                .sorted(Comparator.comparingInt(ResultMessage::sectionId))
                .forEach(r -> {
                    if (anonymized.length() > 0) {
                        anonymized.append("\n\n");
                    }
                    anonymized.append(r.anonymizedText());
                    allSentences.addAll(r.sortedSentences());
                });

        allSentences.sort(Comparator.comparingInt(String::length));

        return new AggregatedJobResult(
                jobId,
                expectedSections,
                totalWordCount,
                globalTop,
                avgSentiment,
                anonymized.toString(),
                allSentences
        );
    }

    private List<AggregatedJobResult.WordCount> computeTopN(
            Map<String, Long> freq, int n) {

        return freq.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed()
                        .thenComparing(Map.Entry.comparingByKey()))
                .limit(n)
                .map(e -> new AggregatedJobResult.WordCount(e.getKey(), e.getValue()))
                .toList();
    }
}
