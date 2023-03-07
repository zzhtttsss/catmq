package org.catmq.broker.common;

import lombok.Getter;
import lombok.Setter;
import org.catmq.broker.topic.Subscription;
import org.catmq.entity.ConsumerBatchPolicy;
import org.catmq.entity.TopicDetail;
import org.catmq.protocol.definition.NumberedMessage;
import org.catmq.protocol.definition.OriginMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

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

    private final int maxQueueSize = 100;

    private final AtomicInteger queueSize = new AtomicInteger(0);

    /**
     * Non-blocking message queue for consumers to pull
     */
    private final ConcurrentLinkedQueue<NumberedMessage> messageQueue;


    public void sendMessages(List<NumberedMessage> msg) {
        if (queueSize.get() + msg.size() > maxQueueSize) {
            // Discard these messages because there are too many old messages not consumed yet
            return;
        }
        queueSize.addAndGet(msg.size());
        this.messageQueue.addAll(msg);
    }

    public void sendOriginMessages(List<OriginMessage> msg) {
        msg.forEach(om -> {
            this.messageQueue.add(NumberedMessage.newBuilder().setBody(om.getBody()).build());
        });

    }

    public List<NumberedMessage> getBatchMessage(ConsumerBatchPolicy policy) {
        int batchNumber = policy.getBatchNumber();
        List<NumberedMessage> messages = new ArrayList<>(batchNumber);
        long endTime = System.currentTimeMillis() + policy.getTimeoutInMs();
        while (messages.size() < batchNumber && System.currentTimeMillis() < endTime) {
            NumberedMessage message = this.messageQueue.poll();
            if (message != null) {
                messages.add(message);
            } else {
                // condition wait
                subscription.notifyConsume();
            }
        }
        return messages;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
        this.partitionIdx = TopicDetail.getPartitionIndex(topicName);
    }

    public Consumer(Subscription subscription, String topicName, long consumerId,
                    String consumerName) {

        this.subscription = subscription;
        this.topicName = topicName;
        this.partitionIdx = TopicDetail.getPartitionIndex(topicName);
        this.consumerId = consumerId;
        this.consumerName = consumerName;
        this.messageQueue = new ConcurrentLinkedQueue<>();
    }

    public Consumer(long consumerId) {
        this.consumerId = consumerId;
        this.messageQueue = new ConcurrentLinkedQueue<>();
    }
}
