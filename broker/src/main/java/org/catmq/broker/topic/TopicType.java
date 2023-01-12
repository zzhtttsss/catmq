package org.catmq.broker.topic;

import lombok.Getter;

public enum TopicType {
    /**
     * Non-persistent topic type
     */
    NON_PERSISTENT("non-persistent"),
    /**
     * Persistent topic type
     */
    PERSISTENT("persistent");

    @Getter
    private final String name;

    public static TopicType fromString(String name) {
        for (TopicType type : TopicType.values()) {
            if (type.getName().equals(name)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown topic type: " + name);
    }

    TopicType(String name) {
        this.name = name;
    }
}
