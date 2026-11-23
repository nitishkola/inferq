package dev.inferq.queue;

import dev.inferq.config.InferqProperties;
import java.util.List;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/** Per-tenant token bucket, enforced in Redis so it holds across API replicas. */
@Component
public class RateLimiter {

    public record Decision(boolean allowed, long retryAfterMs) {}

    private final StringRedisTemplate redis;
    private final LuaScripts scripts;
    private final InferqProperties props;

    public RateLimiter(StringRedisTemplate redis, LuaScripts scripts, InferqProperties props) {
        this.redis = redis;
        this.scripts = scripts;
        this.props = props;
    }

    public Decision tryAcquire(String tenant) {
        InferqProperties.RateLimit rl = props.rateLimit();
        if (!rl.enabled()) {
            return new Decision(true, 0);
        }
        List<Object> r = redis.execute(scripts.rateLimit, List.of("iq:rl:" + tenant),
                String.valueOf(rl.capacity()), String.valueOf(rl.refillPerSecond()), "1");
        return new Decision((Long) r.get(0) == 1L, (Long) r.get(1));
    }
}
