package org.catmq.broker.common;

import lombok.Getter;
import lombok.Setter;
import org.catmq.broker.topic.Subscription;
import org.catmq.broker.topic.TopicName;

import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A Consumer is a consumer currently connected and associated with a Subscription.
 */
@Getter
public class Consumer {
    @Setter
    private Subscription subscription;
    private String topicName;
    private int partitionIdx;
    private final long consumerId;
    @Setter
    private String consumerName;

    /**
     * Non-blocking message queue for consumers to pull
     */
    private final ConcurrentLinkedQueue<String> messageQueue;


    public void sendMessages(String msg) {
        this.messageQueue.offer(msg);
    }

    public Optional<String> getMessage() {
        return Optional.ofNullable(this.messageQueue.poll());
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
        this.partitionIdx = TopicName.getPartitionIndex(topicName);
    }

    public Consumer(Subscription subscription, String topicName, long consumerId,
                    String consumerName) {

        this.subscription = subscription;
        this.topicName = topicName;
        this.partitionIdx = TopicName.getPartitionIndex(topicName);
        this.consumerId = consumerId;
        this.consumerName = consumerName;
        this.messageQueue = new ConcurrentLinkedQueue<>();
    }

    public Consumer(long consumerId) {
        this.consumerId = consumerId;
        this.messageQueue = new ConcurrentLinkedQueue<>();
    }
}
