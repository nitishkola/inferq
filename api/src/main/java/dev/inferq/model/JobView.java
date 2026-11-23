package dev.inferq.model;

import java.util.Map;

/** What the API returns for a job. Built from the job's Redis hash. */
public record JobView(
        String id,
        String tenant,
        String model,
        Priority priority,
        JobStatus status,
        int attempts,
        int maxAttempts,
        String result,
        String lastError,
        String worker,
        Long createdAt,
        Long startedAt,
        Long finishedAt,
        Long queueWaitMs,
        Long totalLatencyMs) {

    public static JobView from(Map<String, String> h) {
        Long created = parseLong(h.get("createdAt"));
        Long started = parseLong(h.get("startedAt"));
        Long finished = parseLong(h.get("finishedAt"));
        Long wait = created != null && started != null ? started - created : null;
        Long total = created != null && finished != null ? finished - created : null;
        return new JobView(
                h.get("id"),
                h.get("tenant"),
                h.get("model"),
                Priority.fromLevel(Integer.parseInt(h.getOrDefault("priority", "1"))),
                JobStatus.valueOf(h.get("status")),
                Integer.parseInt(h.getOrDefault("attempts", "0")),
                Integer.parseInt(h.getOrDefault("maxAttempts", "0")),
                h.get("result"),
                h.get("lastError"),
                h.get("worker"),
                created,
                started,
                finished,
                wait,
                total);
    }

    private static Long parseLong(String s) {
        return s == null || s.isEmpty() ? null : Long.parseLong(s);
    }
}
