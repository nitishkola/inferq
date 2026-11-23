package dev.inferq.model;

public enum JobStatus {
    QUEUED,
    RUNNING,
    RETRY_WAIT,
    SUCCEEDED,
    DEAD,
    CANCELLED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == DEAD || this == CANCELLED;
    }
}
