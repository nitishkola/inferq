package dev.inferq.model;

import java.util.Map;

public record QueueStats(
        Map<String, Long> readyByModel,
        long inflight,
        long retryWaiting,
        long deadLettered,
        Map<String, Long> counters) {}
