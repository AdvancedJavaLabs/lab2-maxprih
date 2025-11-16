package org.itmo.producer.service;

import org.itmo.common.SectionMessage;
import org.itmo.common.TaskType;
import org.itmo.producer.messaging.SectionProducer;
import org.itmo.producer.text.TextSplitter;
import org.springframework.stereotype.Service;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Service
public class JobStarterService {

    private final SectionProducer sectionProducer;

    public JobStarterService(SectionProducer sectionProducer) {
        this.sectionProducer = sectionProducer;
    }

    public String startJob(String text, int topN, int sentencesPerSection) {
        String jobId = UUID.randomUUID().toString();

        TextSplitter splitter = new TextSplitter(sentencesPerSection);
        List<String> sections = splitter.splitIntoSections(text);

        int totalSections = sections.size();
        Set<TaskType> tasks = EnumSet.allOf(TaskType.class);

        int sectionId = 0;
        for (String section : sections) {
            SectionMessage message = new SectionMessage(
                    jobId,
                    sectionId++,
                    totalSections,
                    topN,
                    section,
                    List.copyOf(tasks)
            );
            sectionProducer.sendSection(message);
        }

        return jobId;
    }
}
