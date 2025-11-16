package org.itmo.common;

import java.util.List;

public record SectionMessage(
        String jobId,
        int sectionId,
        int totalSections,
        int topN,
        String text,
        List<TaskType> tasks
) {}