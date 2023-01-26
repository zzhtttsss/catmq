package org.catmq.entity;

import lombok.Getter;

public enum TopicMode {
    /**
     * normal topic mode
     */
    NORMAL("normal"),
    /**
     * ordered topic mode
     */
    ORDERED("ordered");

    @Getter
    private final String name;

    public static TopicMode fromString(String name) {
        for (TopicMode mode : TopicMode.values()) {
            if (mode.getName().equals(name)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unknown topic mode: " + name);
    }

    TopicMode(String name) {
        this.name = name;
    }
}
