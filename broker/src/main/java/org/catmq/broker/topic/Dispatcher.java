package org.catmq.broker.topic;

import org.catmq.broker.common.Consumer;
import org.catmq.broker.common.NumberedMessageBatch;

import java.util.List;

public interface Dispatcher {
    /**
     * Add a consumer to the dispatcher.
     * <strong>String is temporary</strong>
     *
     * @param consumer consumer
     */
    void addConsumer(Consumer consumer);

    /**
     * Remove a consumer from the dispatcher.
     *
     * @param consumer consumer
     */
    void removeConsumer(Consumer consumer);

    /**
     * Get all consumers from the dispatcher.
     *
     * @return consumers
     */
    List<Consumer> getConsumers();

    boolean isActiveConsumer(Consumer consumer);

    /**
     * Send consume message to consumer queue.
     *
     * @param entryBatch
     */
    void sendMessage4Consuming(NumberedMessageBatch entryBatch);

}
