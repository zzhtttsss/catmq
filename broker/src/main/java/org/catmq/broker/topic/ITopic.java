package org.catmq.broker.topic;

public interface ITopic {

    /**
     * put message to the consumers who have this topic
     *
     * @param message message
     */
    void putMessage(String message);

    void subscribe(String subscriptionName, long consumerId);

    ISubscription createSubscription(String subscriptionName);

    String getTopicName();
    
    boolean isSubscribe(String subscriptionName, long consumerId);
}
