package org.catmq.broker.topic;

import org.catmq.protocol.definition.OriginMessage;
import org.catmq.protocol.service.SendMessage2BrokerResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface Topic {

    /**
     * put message to the consumers who have this topic
     */
    CompletableFuture<SendMessage2BrokerResponse> putMessage(List<OriginMessage> messages);

    void subscribe(String subscriptionName, long consumerId);

    Subscription getOrCreateSubscription(String subscriptionName);

    String getTopicName();

    boolean isSubscribe(String subscriptionName, long consumerId);
}
