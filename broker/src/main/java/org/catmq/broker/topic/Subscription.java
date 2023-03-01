package org.catmq.broker.topic;

import org.catmq.broker.common.Consumer;

import java.util.Optional;

public interface Subscription {
    Topic getTopic();

    String getName();

    String getTopicName();

    Optional<Dispatcher> getDispatcher();

    void addConsumer(Consumer consumer);

    void notifyConsume();
}
