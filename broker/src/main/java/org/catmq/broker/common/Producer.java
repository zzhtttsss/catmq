package org.catmq.broker.common;

import lombok.Getter;
import org.catmq.broker.topic.ITopic;

/**
 * Represents a currently connected producer.
 */
@Getter
public class Producer {
    private final ITopic topic;
    private final String producerName;
    private final long producerId;


    public void publishMessage(long producerId, long sequenceId, String message) {
        topic.putMessage(message);
    }

    public Producer(ITopic topic, String producerName, long producerId) {
        this.topic = topic;
        this.producerName = producerName;
        this.producerId = producerId;
    }
}
