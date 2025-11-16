package org.itmo.worker.service;

import lombok.extern.slf4j.Slf4j;
import org.itmo.common.ResultMessage;
import org.itmo.common.SectionMessage;
import org.itmo.worker.sentiment.SentimentLexicon;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Slf4j
public class TextProcessingService {

    private static final Pattern WORD_PATTERN = Pattern.compile("\\p{L}+");

    private static final Pattern NAME_PATTERN_LATIN = Pattern.compile("\\b[A-Z][a-z]+\\b");
    private static final Pattern NAME_PATTERN_CYRILLIC = Pattern.compile("\\b[А-ЯЁ][а-яё]+\\b");

    private static final Pattern SENTENCE_SPLIT_PATTERN =
            Pattern.compile("(?<=[.!?])\\s+");

    private final SentimentLexicon sentimentLexicon;

    public TextProcessingService(SentimentLexicon sentimentLexicon) {
        this.sentimentLexicon = sentimentLexicon;
    }

    public ResultMessage processSection(SectionMessage msg) {
        String text = msg.text();

        Map<String, Long> freq = countWordFrequencies(text);
        long totalWords = freq.values().stream().mapToLong(Long::longValue).sum();

        List<ResultMessage.WordCount> topWords =
                computeTopN(freq, msg.topN());

        double sentimentScore = computeSentiment(freq, totalWords);

        String anonymized = anonymizeNames(text);

        List<String> sortedSentences = sortSentencesByLength(text);

        return new ResultMessage(
                msg.jobId(),
                msg.sectionId(),
                msg.totalSections(),
                msg.topN(),
                totalWords,
                freq,
                topWords,
                sentimentScore,
                anonymized,
                sortedSentences
        );
    }


    private Map<String, Long> countWordFrequencies(String text) {
        Map<String, Long> freq = new HashMap<>();
        Matcher m = WORD_PATTERN.matcher(text.toLowerCase(Locale.ROOT));
        while (m.find()) {
            String word = m.group();
            freq.merge(word, 1L, Long::sum);
        }
        return freq;
    }

    private List<ResultMessage.WordCount> computeTopN(Map<String, Long> freq, int n) {
        return freq.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed()
                        .thenComparing(Map.Entry.comparingByKey()))
                .limit(n)
                .map(e -> new ResultMessage.WordCount(e.getKey(), e.getValue()))
                .toList();
    }

    private double computeSentiment(Map<String, Long> freq, long totalWords) {
        if (totalWords == 0) {
            return 0.0;
        }
        long pos = 0;
        long neg = 0;
        for (Map.Entry<String, Long> e : freq.entrySet()) {
            String w = e.getKey();
            long count = e.getValue();
            if (sentimentLexicon.isPositive(w)) {
                pos += count;
            } else if (sentimentLexicon.isNegative(w)) {
                neg += count;
            }
        }
        return (double) (pos - neg) / totalWords;
    }

    private String anonymizeNames(String text) {
        String result = replaceWithPattern(text, NAME_PATTERN_LATIN, "NAME");
        result = replaceWithPattern(result, NAME_PATTERN_CYRILLIC, "ИМЯ");
        return result;
    }

    private String replaceWithPattern(String text, Pattern pattern, String replacement) {
        Matcher m = pattern.matcher(text);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            m.appendReplacement(sb, replacement);
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private List<String> sortSentencesByLength(String text) {
        String[] raw = SENTENCE_SPLIT_PATTERN.split(text);
        List<String> sentences = new ArrayList<>();
        for (String s : raw) {
            if (!s.isBlank()) {
                sentences.add(s.trim());
            }
        }
        sentences.sort(Comparator.comparingInt(String::length));
        return sentences;
    }
}
