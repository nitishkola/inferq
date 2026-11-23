package dev.inferq.queue;

public class UnknownModelException extends RuntimeException {
    public UnknownModelException(String model) {
        super("unknown model '" + model + "'");
    }
}
