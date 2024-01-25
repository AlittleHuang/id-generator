package io.github.genie.id.generator.core.support;

public class Locked {

    private final int id;
    private final long until;

    public Locked(int id, long until) {
        this.until = until;
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public long getUntil() {
        return until;
    }
}
