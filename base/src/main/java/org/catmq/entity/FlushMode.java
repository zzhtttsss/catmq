package org.catmq.entity;

import lombok.Getter;

public enum FlushMode {
    /**
     * normal topic mode
     */
    SYNC("sync"),
    /**
     * ordered topic mode
     */
    ASYNC("async");

    @Getter
    private final String name;

    public static FlushMode fromString(String name) {
        for (FlushMode mode : FlushMode.values()) {
            if (mode.getName().equals(name)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unknown flush mode: " + name);
    }

    FlushMode(String name) {
        this.name = name;
    }
}
