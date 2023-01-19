package org.catmq.broker.topic;

import org.catmq.broker.common.Consumer;

import java.util.Optional;

public interface ISubscription {
    ITopic getTopic();

    String getName();

    String getTopicName();

    Optional<IDispatcher> getDispatcher();

    void addConsumer(Consumer consumer);
}
