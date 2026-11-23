package dev.inferq.metrics;

import dev.inferq.model.QueueStats;
import dev.inferq.queue.QueueService;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Publishes queue state to Prometheus. State is read from Redis on a timer rather than on
 * every scrape, so a slow Redis can't stall the metrics endpoint.
 */
@Component
public class QueueMetrics {

    private final MeterRegistry registry;
    private final QueueService queue;
    private final Map<String, AtomicLong> ready = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> events = new ConcurrentHashMap<>();
    private final AtomicLong inflight = new AtomicLong();
    private final AtomicLong retryWaiting = new AtomicLong();
    private final AtomicLong deadLettered = new AtomicLong();

    public QueueMetrics(MeterRegistry registry, QueueService queue) {
        this.registry = registry;
        this.queue = queue;
        Gauge.builder("inferq.queue.inflight", inflight, AtomicLong::get).register(registry);
        Gauge.builder("inferq.queue.retry_waiting", retryWaiting, AtomicLong::get).register(registry);
        Gauge.builder("inferq.queue.dead_lettered", deadLettered, AtomicLong::get).register(registry);
    }

    @Scheduled(fixedDelay = 2000)
    public void refresh() {
        QueueStats s;
        try {
            s = queue.stats();
        } catch (Exception e) {
            return; // keep last known values
        }
        inflight.set(s.inflight());
        retryWaiting.set(s.retryWaiting());
        deadLettered.set(s.deadLettered());
        s.readyByModel().forEach((model, depth) -> ready.computeIfAbsent(model, m -> {
            AtomicLong a = new AtomicLong();
            Gauge.builder("inferq.queue.ready", a, AtomicLong::get).tag("model", m).register(registry);
            return a;
        }).set(depth));
        s.counters().forEach((event, count) -> events.computeIfAbsent(event, e -> {
            AtomicLong a = new AtomicLong();
            FunctionCounter.builder("inferq.jobs", a, AtomicLong::get).tag("event", e).register(registry);
            return a;
        }).set(count));
    }
}
