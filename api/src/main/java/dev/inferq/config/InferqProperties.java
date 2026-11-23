package dev.inferq.config;

import dev.inferq.model.Priority;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "inferq")
public record InferqProperties(
        List<String> models,
        int maxQueueDepth,
        int defaultMaxAttempts,
        long baseBackoffMs,
        long maxBackoffMs,
        int idempotencyTtlSeconds,
        int resultTtlSeconds,
        long maintenanceIntervalMs,
        int maintenanceBatch,
        Map<Priority, Long> priorityPenaltyMs,
        RateLimit rateLimit) {

    public record RateLimit(boolean enabled, double capacity, double refillPerSecond) {}

    public String defaultModel() {
        return models.get(0);
    }

    public long penaltyFor(Priority priority) {
        return priorityPenaltyMs.getOrDefault(priority, 0L);
    }
}
