package org.itmo.aggregator.model;

import java.util.List;

public record AggregatedJobResult(
        String jobId,
        int totalSections,
        long totalWordCount,
        List<WordCount> topWords,
        double averageSentiment,
        String anonymizedText,
        List<String> sortedSentences
) {
    public record WordCount(String word, long count) {}
}
