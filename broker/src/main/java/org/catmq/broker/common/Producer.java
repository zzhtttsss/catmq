package org.catmq.broker.common;

import lombok.Getter;
import org.catmq.broker.topic.Topic;

/**
 * Represents a currently connected producer.
 */
@Getter
public class Producer {
    private final Topic topic;
    private final String producerName;
    private final long producerId;


    public void publishMessage(long producerId, long sequenceId, String message) {
//        topic.putMessage(message.getBytes());
    }

    public Producer(Topic topic, String producerName, long producerId) {
        this.topic = topic;
        this.producerName = producerName;
        this.producerId = producerId;
    }
}
