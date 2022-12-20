package org.catmq.context;

public class Context {
    public String type;
    public String tenant;
    public String namespace;
    public String testField;

    public Context(String type, String testField) {
        this.type = type;
        this.testField = testField;
    }
}
