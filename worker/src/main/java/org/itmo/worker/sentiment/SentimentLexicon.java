package org.itmo.worker.sentiment;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

@Component
public class SentimentLexicon {

    private final Set<String> positiveWords;
    private final Set<String> negativeWords;

    public SentimentLexicon(
            @Value("${sentiment.lexicon.positive:classpath:sentiment/positive.txt}")
            Resource positiveResource,
            @Value("${sentiment.lexicon.negative:classpath:sentiment/negative.txt}")
            Resource negativeResource
    ) {
        this.positiveWords = loadWords(positiveResource);
        this.negativeWords = loadWords(negativeResource);
    }

    public boolean isPositive(String word) {
        return positiveWords.contains(word);
    }

    public boolean isNegative(String word) {
        return negativeWords.contains(word);
    }

    public Set<String> getPositiveWords() {
        return positiveWords;
    }

    public Set<String> getNegativeWords() {
        return negativeWords;
    }

    private Set<String> loadWords(Resource resource) {
        if (resource == null) {
            return Collections.emptySet();
        }
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8))) {
            return reader.lines()
                    .map(String::trim)
                    .map(line -> {
                        int commentIdx = line.indexOf('#');
                        return commentIdx >= 0 ? line.substring(0, commentIdx).trim() : line;
                    })
                    .filter(line -> !line.isEmpty())
                    .map(line -> line.toLowerCase())
                    .collect(Collectors.toCollection(() -> new TreeSet<>(String::compareTo)));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load sentiment lexicon from " + resource, e);
        }
    }
}

