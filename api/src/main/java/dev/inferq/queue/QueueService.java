package dev.inferq.queue;

import dev.inferq.config.InferqProperties;
import dev.inferq.model.JobView;
import dev.inferq.model.Priority;
import dev.inferq.model.QueueStats;
import dev.inferq.model.SubmitJobRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

@Service
public class QueueService {

    static final String JOB = "iq:job:";
    static final String READY = "iq:ready:";
    static final String INFLIGHT = "iq:inflight";
    static final String DELAYED = "iq:delayed";
    static final String DLQ = "iq:dlq";
    static final String MODELS = "iq:models";
    static final String STATS = "iq:stats";

    public sealed interface SubmitResult permits Accepted, Duplicate, QueueFull {}

    public record Accepted(String id) implements SubmitResult {}

    public record Duplicate(String id) implements SubmitResult {}

    public record QueueFull() implements SubmitResult {}

    public record ReapResult(long reaped, long deadLettered) {}

    private final StringRedisTemplate redis;
    private final LuaScripts scripts;
    private final InferqProperties props;

    public QueueService(StringRedisTemplate redis, LuaScripts scripts, InferqProperties props) {
        this.redis = redis;
        this.scripts = scripts;
        this.props = props;
    }

    public SubmitResult submit(String tenant, SubmitJobRequest req, String idempotencyKey) {
        String model = resolveModel(req.model());
        Priority priority = req.priority() == null ? Priority.NORMAL : req.priority();
        int maxTokens = req.maxTokens() == null ? 256 : req.maxTokens();
        int maxAttempts = req.maxAttempts() == null ? props.defaultMaxAttempts() : req.maxAttempts();
        String id = UUID.randomUUID().toString();

        List<Object> r = run(scripts.submit,
                id, tenant, model, priority.level(), req.prompt(), maxTokens, maxAttempts,
                idempotencyKey == null ? "" : idempotencyKey,
                props.idempotencyTtlSeconds(), props.maxQueueDepth(), props.penaltyFor(priority));

        String outcome = (String) r.get(0);
        return switch (outcome) {
            case "OK" -> new Accepted(id);
            case "DUPLICATE" -> new Duplicate((String) r.get(1));
            case "FULL" -> new QueueFull();
            default -> throw new IllegalStateException("unexpected submit result " + outcome);
        };
    }

    public Optional<JobView> get(String id) {
        Map<Object, Object> raw = redis.opsForHash().entries(JOB + id);
        if (raw.isEmpty()) {
            return Optional.empty();
        }
        Map<String, String> h = new HashMap<>();
        raw.forEach((k, v) -> h.put(k.toString(), v.toString()));
        return Optional.of(JobView.from(h));
    }

    /** Returns the status the job had before cancelling, or "NOT_FOUND". */
    public String cancel(String id) {
        return run(scripts.cancel, id);
    }

    public boolean redrive(String id) {
        Long n = run(scripts.redrive, id);
        return n != null && n == 1L;
    }

    public List<JobView> deadLetters(int limit) {
        List<String> ids = redis.opsForList().range(DLQ, 0, limit - 1);
        List<JobView> out = new ArrayList<>();
        if (ids != null) {
            for (String id : ids) {
                get(id).ifPresent(out::add);
            }
        }
        return out;
    }

    public long promoteDueRetries() {
        long total = 0;
        long n;
        do {
            n = run(scripts.promote, props.maintenanceBatch());
            total += n;
        } while (n == props.maintenanceBatch());
        return total;
    }

    public ReapResult reapExpiredLeases() {
        List<Object> r = run(scripts.reap,
                props.maintenanceBatch(), props.baseBackoffMs(), props.maxBackoffMs(),
                ThreadLocalRandom.current().nextDouble());
        return new ReapResult((Long) r.get(0), (Long) r.get(1));
    }

    public QueueStats stats() {
        Set<String> models = redis.opsForSet().members(MODELS);
        Map<String, Long> ready = new TreeMap<>();
        for (String m : props.models()) {
            ready.put(m, 0L);
        }
        if (models != null) {
            for (String m : models) {
                ready.put(m, nz(redis.opsForZSet().zCard(READY + m)));
            }
        }
        Map<String, Long> counters = new TreeMap<>();
        redis.opsForHash().entries(STATS)
                .forEach((k, v) -> counters.put(k.toString(), Long.parseLong(v.toString())));
        return new QueueStats(
                ready,
                nz(redis.opsForZSet().zCard(INFLIGHT)),
                nz(redis.opsForZSet().zCard(DELAYED)),
                nz(redis.opsForList().size(DLQ)),
                counters);
    }

    private String resolveModel(String requested) {
        if (requested == null || requested.isBlank()) {
            return props.defaultModel();
        }
        if (!props.models().contains(requested)) {
            throw new UnknownModelException(requested);
        }
        return requested;
    }

    private <T> T run(RedisScript<T> script, Object... args) {
        Object[] strArgs = new Object[args.length];
        for (int i = 0; i < args.length; i++) {
            strArgs[i] = String.valueOf(args[i]);
        }
        return redis.execute(script, List.of(), strArgs);
    }

    private static long nz(Long v) {
        return v == null ? 0L : v;
    }
}
