package org.catmq.broker.topic.persistent;

import org.catmq.broker.common.Consumer;
import org.catmq.broker.common.NumberedMessageBatch;
import org.catmq.broker.topic.Dispatcher;
import org.catmq.entity.BrokerConfig;

import java.util.Collections;
import java.util.List;

public class SingleConsumerPersistentDispatcher implements Dispatcher {
    protected volatile int readBatchSize;

    private volatile Consumer activeConsumer = null;

    private final PersistentTopic topic;


    @Override
    public void addConsumer(Consumer consumer) {
        this.activeConsumer = consumer;
    }

    @Override
    public void removeConsumer(Consumer consumer) {
        this.activeConsumer = null;
    }

    @Override
    public List<Consumer> getConsumers() {
        return Collections.singletonList(activeConsumer);
    }

    @Override
    public boolean isActiveConsumer(Consumer consumer) {
        return this.activeConsumer.getConsumerId() == consumer.getConsumerId();
    }

    @Override
    public void sendMessage4Consuming(NumberedMessageBatch entryBatch) {
        if (activeConsumer != null) {
            activeConsumer.sendMessages(entryBatch.getBatch());
        }
    }

    public SingleConsumerPersistentDispatcher(PersistentTopic topic) {
        this.topic = topic;
        this.readBatchSize = BrokerConfig.BROKER_CONFIG.getMaxReadBatchSize();
    }
}
