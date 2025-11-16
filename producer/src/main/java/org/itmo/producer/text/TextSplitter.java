package org.itmo.producer.text;

import java.util.ArrayList;
import java.util.List;

public class TextSplitter {

    private final int sentencesPerSection;

    public TextSplitter(int sentencesPerSection) {
        this.sentencesPerSection = sentencesPerSection;
    }

    public List<String> splitIntoSections(String text) {
        List<String> sentences = splitIntoSentences(text);
        List<String> sections = new ArrayList<>();

        StringBuilder current = new StringBuilder();
        int count = 0;

        for (String sentence : sentences) {
            if (sentence.isBlank()) {
                continue;
            }
            if (current.length() > 0) {
                current.append(' ');
            }
            current.append(sentence.trim());
            count++;

            if (count >= sentencesPerSection) {
                sections.add(current.toString());
                current.setLength(0);
                count = 0;
            }
        }

        if (current.length() > 0) {
            sections.add(current.toString());
        }

        return sections;
    }

    private List<String> splitIntoSentences(String text) {
        String[] raw = text.split("(?<=[.!?])\\s+");
        List<String> sentences = new ArrayList<>(raw.length);
        for (String s : raw) {
            if (!s.isBlank()) {
                sentences.add(s.trim());
            }
        }
        return sentences;
    }
}
