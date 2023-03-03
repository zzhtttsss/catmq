package org.catmq.broker.topic.persistent;

import org.catmq.broker.common.Consumer;
import org.catmq.broker.common.NumberedMessageBatch;
import org.catmq.broker.topic.Dispatcher;

import java.util.List;

public class PersistentDispatcher implements Dispatcher {
    @Override
    public void addConsumer(Consumer consumer) {

    }

    @Override
    public void removeConsumer(Consumer consumer) {

    }

    @Override
    public List<Consumer> getConsumers() {
        return null;
    }

    @Override
    public boolean isActiveConsumer(Consumer consumer) {
        return false;
    }

    @Override
    public void sendMessage4Consuming(NumberedMessageBatch entryBatch) {

    }
}
