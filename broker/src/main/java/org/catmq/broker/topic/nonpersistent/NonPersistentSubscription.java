package org.catmq.broker.topic.nonpersistent;

import lombok.Getter;
import org.catmq.broker.common.Consumer;
import org.catmq.broker.topic.Dispatcher;
import org.catmq.broker.topic.Subscription;

import java.util.Optional;

public class NonPersistentSubscription implements Subscription {
    @Getter
    private final NonPersistentTopic topic;

    private final String subscriptionName;
    @Getter
    private final String topicName;
    private volatile NonPersistentDispatcher dispatcher;

    @Override
    public String getName() {
        return subscriptionName;
    }

    @Override
    public void addConsumer(Consumer consumer) {
        if (dispatcher == null) {
            dispatcher = new SingleConsumerNonPersistentDispatcher(this, topic);
        }
        dispatcher.addConsumer(consumer);
    }

    @Override
    public void notifyConsume() {
        throw new UnsupportedOperationException("Not support notify consume.");
    }

    @Override
    public Optional<Dispatcher> getDispatcher() {
        return Optional.ofNullable(this.dispatcher);
    }

    public boolean isSubscribe() {
        return this.dispatcher != null;
    }

    public boolean isActiveConsumer(Consumer consumer) {
        return dispatcher.isActiveConsumer(consumer);
    }

    public NonPersistentSubscription(NonPersistentTopic topic, String subscriptionName) {
        this.topic = topic;
        this.topicName = topic.getTopicName();
        this.subscriptionName = subscriptionName;
    }
}
