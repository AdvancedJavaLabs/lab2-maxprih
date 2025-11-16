package org.itmo.common;

import java.util.List;
import java.util.Map;

public record ResultMessage(
        String jobId,
        int sectionId,
        int totalSections,
        int topN,
        long wordCount,
        Map<String, Long> wordFreq,
        List<WordCount> topWords,
        double sentimentScore,
        String anonymizedText,
        List<String> sortedSentences
) {
    public record WordCount(String word, long count) {}
}