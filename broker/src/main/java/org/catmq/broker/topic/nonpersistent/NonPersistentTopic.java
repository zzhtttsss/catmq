package org.catmq.broker.topic.nonpersistent;

import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.common.Consumer;
import org.catmq.broker.manager.ClientManager;
import org.catmq.broker.topic.BaseTopic;
import org.catmq.broker.topic.Subscription;
import org.catmq.broker.topic.Topic;
import org.catmq.entity.TopicDetail;
import org.catmq.protocol.definition.OriginMessage;
import org.catmq.protocol.service.SendMessage2BrokerResponse;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.catmq.broker.Broker.BROKER;

@Slf4j
public class NonPersistentTopic extends BaseTopic implements Topic {

    /**
     * Subscriptions to this topic whose key is subscriptionName
     */
    private final ConcurrentHashMap<String, NonPersistentSubscription> subscriptions;

    private final ClientManager clientManager;

    @Override
    public CompletableFuture<SendMessage2BrokerResponse> putMessage(List<OriginMessage> messages) {
        log.warn("non-persistent!");
        subscriptions.forEach((name, subscription) -> {
            subscription.getDispatcher().ifPresent(dispatcher -> {
                if (dispatcher instanceof SingleConsumerNonPersistentDispatcher singleActiveConsumer) {
                    singleActiveConsumer.sendMessages(messages);
                } else {
                    log.error("Unknown dispatcher type {}", dispatcher.getClass());
                }
            });
        });
        return new CompletableFuture<>();
    }

    @Override
    public void subscribe(String subscriptionName, long consumerId) {
        log.info("[{}][{}] Created new subscription for {}", topicName, subscriptionName, consumerId);
        NonPersistentSubscription subscription = subscriptions.computeIfAbsent(subscriptionName,
                name -> new NonPersistentSubscription(this, subscriptionName));
        Consumer consumer = clientManager.getConsumer(consumerId);
        consumer.setSubscription(subscription);
        consumer.setTopicName(topicName);
        subscription.addConsumer(consumer);
    }

    @Override
    public Subscription getOrCreateSubscription(String subscriptionName) {
        log.info("[{}] topic created new subscription [{}]", topicName, subscriptionName);
        return this.subscriptions.computeIfAbsent(subscriptionName,
                name -> new NonPersistentSubscription(this, subscriptionName));
    }

    @Override
    public String getTopicName() {
        return super.topicName;
    }

    @Override
    public boolean isSubscribe(String subscriptionName, long consumerId) {
        NonPersistentSubscription subscription = subscriptions.get(subscriptionName);
        if (subscription == null) {
            return false;
        }
        return subscription
                .getDispatcher()
                .map(dispatcher -> dispatcher.isActiveConsumer(clientManager.getConsumer(consumerId)))
                .orElse(false);
    }


    public NonPersistentTopic(TopicDetail topicDetail) {
        super(topicDetail);
        this.subscriptions = new ConcurrentHashMap<>();
        this.clientManager = BROKER.getClientManager();
    }
}
