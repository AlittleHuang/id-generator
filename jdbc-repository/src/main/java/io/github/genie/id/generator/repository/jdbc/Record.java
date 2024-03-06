package io.github.genie.id.generator.repository.jdbc;

public class Record {
    private final int id;
    private final String key;

    public Record(int id, String key) {
        this.id = id;
        this.key = key;
    }

    public int getId() {
        return id;
    }

    public String getKey() {
        return key;
    }
}
