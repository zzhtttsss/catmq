package org.catmq.broker.topic.persistent;

import lombok.Getter;
import org.catmq.broker.common.Consumer;
import org.catmq.broker.topic.Dispatcher;
import org.catmq.broker.topic.Subscription;
import org.catmq.broker.topic.nonpersistent.NonPersistentDispatcher;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class PersistentSubscription implements Subscription {
    @Getter
    private final PersistentTopic topic;

    private final String subscriptionName;
    @Getter
    private final String topicName;
    private volatile NonPersistentDispatcher dispatcher;

    private AtomicLong LastConsumeSegmentId;

    private AtomicLong LastConsumeEntryId;

    @Override
    public String getName() {
        return subscriptionName;
    }

    @Override
    public void addConsumer(Consumer consumer) {
        if (dispatcher == null) {
            // TODO
        }
        dispatcher.addConsumer(consumer);
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

    public PersistentSubscription(PersistentTopic topic, String subscriptionName) {
        this.topic = topic;
        this.topicName = topic.getTopicName();
        this.subscriptionName = subscriptionName;
    }
}
