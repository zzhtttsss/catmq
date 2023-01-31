package org.catmq.broker.topic;

import lombok.Getter;
import org.catmq.broker.common.Producer;
import org.catmq.entity.TopicDetail;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Used to monitor later
 */
public abstract class BaseTopic implements Topic {
    protected final String topicName;
    @Getter
    private final TopicDetail topicDetail;

    /**
     * Producers currently connected to this topic
     */
    protected final ConcurrentHashMap<String, Producer> producers;

    public BaseTopic(TopicDetail topicDetail) {
        this.topicDetail = topicDetail;
        this.topicName = topicDetail.getCompleteTopicName();
        this.producers = new ConcurrentHashMap<>();
    }

}
