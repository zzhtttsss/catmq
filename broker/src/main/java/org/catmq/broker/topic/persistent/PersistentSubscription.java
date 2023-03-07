package org.catmq.broker.topic.persistent;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.common.Consumer;
import org.catmq.broker.manager.ReadCacheManager;
import org.catmq.broker.manager.TopicManager;
import org.catmq.broker.topic.Dispatcher;
import org.catmq.broker.topic.Subscription;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.catmq.broker.Broker.BROKER;

@Slf4j
public class PersistentSubscription implements Subscription {
    @Getter
    private final PersistentTopic topic;

    private final ReadCacheManager readCacheManager;

    private final TopicManager topicManager;

    private final String subscriptionName;
    @Getter
    private final String topicName;
    private volatile SingleConsumerPersistentDispatcher dispatcher;

    private AtomicLong lastConsumeSegmentId;

    private AtomicLong lastConsumeEntryId;

    @Override
    public String getName() {
        return subscriptionName;
    }

    @Override
    public void addConsumer(Consumer consumer) {
        if (dispatcher == null) {
            dispatcher = new SingleConsumerPersistentDispatcher(topic);
        }
        dispatcher.addConsumer(consumer);
    }


    @Override
    public Optional<Dispatcher> getDispatcher() {
        return Optional.ofNullable(this.dispatcher);
    }

    @Override
    public void notifyConsume() {
        if (dispatcher == null) {
            log.error("Not add consumers yet.");
            return;
        }
        // 1. poll message from cache in broker
        readCacheManager.getEntryBatch(topicName, lastConsumeSegmentId.get(), lastConsumeEntryId.get())
                // 2. poll message from cache in storer
                .or(() -> readCacheManager
                        .getEntryBatchFromStorer(topicName, lastConsumeSegmentId.get(), lastConsumeEntryId.get()))
                .ifPresent(entryBatch -> {
                    int size = entryBatch.getBatch().size();
                    if (size > 0) {
                        lastConsumeSegmentId.set(entryBatch.getSegmentId());
                        lastConsumeEntryId.set(entryBatch.getBatch().get(size - 1).getEntryId() + 1);
                        dispatcher.sendMessage4Consuming(entryBatch);
                    }
                });
        // 3. check whether consume to the end of the segment. If so, move to the next segment.
        if (topicManager.getSegmentTotalNumber(lastConsumeSegmentId.get()) == lastConsumeEntryId.get()) {
            lastConsumeSegmentId.set(topicManager.getSegmentIdNext(topicName, lastConsumeSegmentId.get()));
            lastConsumeEntryId.set(1);
        }
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
        this.readCacheManager = BROKER.getReadCacheManager();
        this.topicManager = BROKER.getTopicManager();
        this.lastConsumeSegmentId = new AtomicLong(topicManager.getSegmentIdFirst(topicName));
        this.lastConsumeEntryId = new AtomicLong(1);
    }
}
