package dev.inferq.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

class JobViewTest {

    @Test
    void computesLatenciesFromTimestamps() {
        JobView v = JobView.from(Map.ofEntries(
                Map.entry("id", "j1"), Map.entry("tenant", "t"), Map.entry("model", "m"),
                Map.entry("priority", "0"), Map.entry("status", "SUCCEEDED"),
                Map.entry("attempts", "2"), Map.entry("maxAttempts", "3"),
                Map.entry("createdAt", "1000"), Map.entry("startedAt", "1250"),
                Map.entry("finishedAt", "1900"), Map.entry("result", "hi")));

        assertThat(v.priority()).isEqualTo(Priority.HIGH);
        assertThat(v.status().isTerminal()).isTrue();
        assertThat(v.queueWaitMs()).isEqualTo(250L);
        assertThat(v.totalLatencyMs()).isEqualTo(900L);
    }

    @Test
    void queuedJobHasNoLatencyYet() {
        JobView v = JobView.from(Map.of(
                "id", "j2", "priority", "2", "status", "QUEUED", "createdAt", "1000"));

        assertThat(v.priority()).isEqualTo(Priority.LOW);
        assertThat(v.queueWaitMs()).isNull();
        assertThat(v.totalLatencyMs()).isNull();
    }
}
