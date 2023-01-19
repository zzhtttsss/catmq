package org.catmq.broker.topic;

import org.catmq.broker.common.Producer;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Used to monitor later
 */
public abstract class BaseTopic implements Topic {
    protected final String topicName;

    /**
     * Producers currently connected to this topic
     */
    protected final ConcurrentHashMap<String, Producer> producers;

    public BaseTopic(String topicName) {
        this.topicName = topicName;
        this.producers = new ConcurrentHashMap<>();
    }

}
