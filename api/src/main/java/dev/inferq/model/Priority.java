package dev.inferq.model;

public enum Priority {
    HIGH(0),
    NORMAL(1),
    LOW(2);

    private final int level;

    Priority(int level) {
        this.level = level;
    }

    public int level() {
        return level;
    }

    public static Priority fromLevel(int level) {
        for (Priority p : values()) {
            if (p.level == level) {
                return p;
            }
        }
        throw new IllegalArgumentException("unknown priority level " + level);
    }
}
