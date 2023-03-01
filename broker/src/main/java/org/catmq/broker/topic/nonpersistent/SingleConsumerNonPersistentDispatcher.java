package org.catmq.broker.topic.nonpersistent;

import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.common.Consumer;
import org.catmq.broker.common.NumberedMessageBatch;
import org.catmq.protocol.definition.OriginMessage;

import java.util.Collections;
import java.util.List;

@Slf4j
public class SingleConsumerNonPersistentDispatcher implements NonPersistentDispatcher {
    private final NonPersistentSubscription subscription;

    //TODO: bug here
    /*protected static final AtomicReferenceFieldUpdater<NonPersistentDispatcherSingleActiveConsumer, Consumer>
            ACTIVE_CONSUMER_UPDATER = AtomicReferenceFieldUpdater.newUpdater(
            NonPersistentDispatcherSingleActiveConsumer.class, Consumer.class, "activeConsumer");*/
    private volatile Consumer activeConsumer = null;

    private final NonPersistentTopic topic;

    @Override
    public synchronized void addConsumer(Consumer consumer) {
        activeConsumer = consumer;
    }

    @Override
    public synchronized void removeConsumer(Consumer consumer) {
        activeConsumer = null;
    }

    @Override
    public List<Consumer> getConsumers() {
        return Collections.singletonList(activeConsumer);
    }

    @Override
    public void sendMessages(List<OriginMessage> msg) {
        Consumer consumer = activeConsumer;
        if (consumer != null) {
            consumer.sendOriginMessages(msg);
        } else {
            //TODO: send to dead letter queue
            log.warn("No active consumer for topic {}", topic);
        }
    }

    @Override
    public boolean isActiveConsumer(Consumer consumer) {
        return activeConsumer.getConsumerId() == consumer.getConsumerId();
    }

    @Override
    public void sendConsume(NumberedMessageBatch entryBatch) {
        throw new UnsupportedOperationException("Single consumer dispatcher does not support sendConsume");
    }

    public SingleConsumerNonPersistentDispatcher(NonPersistentSubscription subscription, NonPersistentTopic topic) {
        this.subscription = subscription;
        this.topic = topic;
    }
}
