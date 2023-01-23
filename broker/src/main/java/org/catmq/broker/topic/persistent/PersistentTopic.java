package org.catmq.broker.topic.persistent;

import org.catmq.broker.topic.BaseTopic;
import org.catmq.broker.topic.Subscription;
import org.catmq.broker.topic.Topic;

public class PersistentTopic extends BaseTopic implements Topic {


    public PersistentTopic(String topicName) {
        super(topicName);
    }

    @Override
    public void putMessage(byte[] message) {

    }

    @Override
    public void subscribe(String subscriptionName, long consumerId) {

    }

    @Override
    public Subscription getOrCreateSubscription(String subscriptionName) {
        return null;
    }

    @Override
    public String getTopicName() {
        return null;
    }

    @Override
    public boolean isSubscribe(String subscriptionName, long consumerId) {
        return false;
    }
}
