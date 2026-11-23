package dev.inferq.queue;

import java.util.List;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * The queue's state transitions are Lua scripts, so each one is atomic inside Redis.
 * The same files (repo-root /lua) are used by the Python workers.
 */
@Component
public class LuaScripts {

    final RedisScript<List<Object>> submit = load("submit", List.class);
    final RedisScript<Long> promote = load("promote", Long.class);
    final RedisScript<List<Object>> reap = load("reap", List.class);
    final RedisScript<String> cancel = load("cancel", String.class);
    final RedisScript<Long> redrive = load("redrive", Long.class);
    final RedisScript<List<Object>> rateLimit = load("ratelimit", List.class);

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> RedisScript<T> load(String name, Class<?> resultType) {
        DefaultRedisScript script = new DefaultRedisScript();
        script.setLocation(new ClassPathResource("lua/" + name + ".lua"));
        script.setResultType(resultType);
        return script;
    }
}
