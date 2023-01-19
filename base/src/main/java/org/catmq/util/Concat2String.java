package org.catmq.util;

public class Concat2String {
    private final StringBuilder sb = new StringBuilder();

    public Concat2String concat(CharSequence cs) {
        sb.append(cs);
        return this;
    }

    public Concat2String concat(Number n) {
        sb.append(n);
        return this;
    }

    public String build() {
        return this.sb.toString();
    }

    public static Concat2String builder() {
        return new Concat2String();
    }

}
