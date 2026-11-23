package dev.inferq.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Background loop: moves retries whose backoff elapsed back to the ready queue, and
 * recovers jobs from workers that stopped heartbeating. Every step is an atomic Lua
 * script, so it is safe to run this in several API replicas at once.
 */
@Component
public class MaintenanceScheduler {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceScheduler.class);

    private final QueueService queue;

    public MaintenanceScheduler(QueueService queue) {
        this.queue = queue;
    }

    @Scheduled(fixedDelayString = "${inferq.maintenance-interval-ms}")
    public void tick() {
        try {
            QueueService.ReapResult reaped = queue.reapExpiredLeases();
            if (reaped.reaped() > 0) {
                log.warn("recovered {} job(s) from expired leases ({} dead-lettered)",
                        reaped.reaped(), reaped.deadLettered());
            }
            long promoted = queue.promoteDueRetries();
            if (promoted > 0) {
                log.debug("promoted {} retr(ies) back to ready", promoted);
            }
        } catch (Exception e) {
            log.warn("maintenance tick failed: {}", e.toString());
        }
    }
}
