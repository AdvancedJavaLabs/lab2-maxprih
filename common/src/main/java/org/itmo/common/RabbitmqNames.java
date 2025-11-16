package org.itmo.common;

public final class RabbitmqNames {

    private RabbitmqNames() {
    }

    public static final String TASKS_EXCHANGE = "text.tasks.exchange";
    public static final String SECTIONS_QUEUE = "text.sections.queue";
    public static final String RESULTS_QUEUE  = "text.results.queue";

    public static final String ROUTING_SECTION = "section";
    public static final String ROUTING_RESULT  = "result";
}
