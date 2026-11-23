package dev.inferq.web;

import dev.inferq.model.JobStatus;
import dev.inferq.model.JobView;
import dev.inferq.model.QueueStats;
import dev.inferq.model.SubmitJobRequest;
import dev.inferq.queue.QueueService;
import dev.inferq.queue.QueueService.Accepted;
import dev.inferq.queue.QueueService.Duplicate;
import dev.inferq.queue.QueueService.QueueFull;
import dev.inferq.queue.RateLimiter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1")
public class JobController {

    private static final Pattern SAFE_ID = Pattern.compile("[A-Za-z0-9_.:-]{1,128}");

    private final QueueService queue;
    private final RateLimiter rateLimiter;
    private final MeterRegistry metrics;

    public JobController(QueueService queue, RateLimiter rateLimiter, MeterRegistry metrics) {
        this.queue = queue;
        this.rateLimiter = rateLimiter;
        this.metrics = metrics;
    }

    @PostMapping("/jobs")
    public ResponseEntity<?> submit(
            @RequestHeader(value = "X-Tenant-Id", defaultValue = "anonymous") String tenant,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @Valid @RequestBody SubmitJobRequest request) {
        requireSafe("X-Tenant-Id", tenant);
        if (idempotencyKey != null) {
            requireSafe("Idempotency-Key", idempotencyKey);
        }

        RateLimiter.Decision decision = rateLimiter.tryAcquire(tenant);
        if (!decision.allowed()) {
            count("rate_limited");
            long retryAfterSec = Math.max(1, (decision.retryAfterMs() + 999) / 1000);
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .header("Retry-After", String.valueOf(retryAfterSec))
                    .body(new ApiError("rate_limited", "tenant '" + tenant + "' is over its rate limit"));
        }

        return switch (queue.submit(tenant, request, idempotencyKey)) {
            case Accepted a -> {
                count("accepted");
                yield ResponseEntity.accepted()
                        .location(URI.create("/v1/jobs/" + a.id()))
                        .body(Map.of("id", a.id(), "status", JobStatus.QUEUED));
            }
            case Duplicate d -> {
                count("duplicate");
                yield ResponseEntity.ok()
                        .header("Idempotent-Replay", "true")
                        .body(Map.of("id", d.id(), "duplicate", true));
            }
            case QueueFull f -> {
                count("queue_full");
                yield ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .header("Retry-After", "1")
                        .body(new ApiError("queue_full", "queue is at capacity, retry later"));
            }
        };
    }

    @GetMapping("/jobs/{id}")
    public ResponseEntity<JobView> get(@PathVariable String id) {
        requireSafe("id", id);
        return queue.get(id).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/jobs/{id}")
    public ResponseEntity<?> cancel(@PathVariable String id) {
        requireSafe("id", id);
        String previous = queue.cancel(id);
        if ("NOT_FOUND".equals(previous)) {
            return ResponseEntity.notFound().build();
        }
        JobStatus prev = JobStatus.valueOf(previous);
        if (prev.isTerminal()) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(new ApiError("already_finished", "job is already " + prev));
        }
        return ResponseEntity.ok(Map.of("id", id, "previousStatus", prev, "status", JobStatus.CANCELLED));
    }

    @GetMapping("/stats")
    public QueueStats stats() {
        return queue.stats();
    }

    @GetMapping("/dlq")
    public List<JobView> deadLetters(@RequestParam(defaultValue = "50") int limit) {
        return queue.deadLetters(Math.min(Math.max(limit, 1), 500));
    }

    @PostMapping("/dlq/{id}/redrive")
    public ResponseEntity<?> redrive(@PathVariable String id) {
        requireSafe("id", id);
        if (queue.redrive(id)) {
            return ResponseEntity.ok(Map.of("id", id, "status", JobStatus.QUEUED));
        }
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new ApiError("not_dead_lettered", "job is not in the dead-letter queue"));
    }

    private void count(String result) {
        metrics.counter("inferq.submissions", "result", result).increment();
    }

    private static void requireSafe(String name, String value) {
        if (!SAFE_ID.matcher(value).matches()) {
            throw new IllegalArgumentException(name + " must match " + SAFE_ID.pattern());
        }
    }
}
