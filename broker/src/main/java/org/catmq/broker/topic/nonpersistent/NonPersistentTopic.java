package org.catmq.broker.topic.nonpersistent;

import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.common.Consumer;
import org.catmq.broker.service.ClientManageService;
import org.catmq.broker.topic.BaseTopic;
import org.catmq.broker.topic.Subscription;
import org.catmq.broker.topic.Topic;

import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class NonPersistentTopic extends BaseTopic implements Topic {

    /**
     * Subscriptions to this topic whose key is subscriptionName
     */
    private final ConcurrentHashMap<String, NonPersistentSubscription> subscriptions;

    private final ClientManageService clientManageService;

    @Override
    public void putMessage(byte[] message) {
        subscriptions.forEach((name, subscription) -> {
            subscription.getDispatcher().ifPresent(dispatcher -> {
                if (dispatcher instanceof SingleActiveConsumerNonPersistentDispatcher singleActiveConsumer) {
                    singleActiveConsumer.sendMessages(message);
                } else {
                    log.error("Unknown dispatcher type {}", dispatcher.getClass());
                }
            });
        });
    }

    @Override
    public void subscribe(String subscriptionName, long consumerId) {
        log.info("[{}][{}] Created new subscription for {}", topicName, subscriptionName, consumerId);
        NonPersistentSubscription subscription = subscriptions.computeIfAbsent(subscriptionName,
                name -> new NonPersistentSubscription(this, subscriptionName));
        Consumer consumer = clientManageService.getConsumer(consumerId);
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
                .map(dispatcher -> dispatcher.isActiveConsumer(clientManageService.getConsumer(consumerId)))
                .orElse(false);
    }


    public NonPersistentTopic(String topic) {
        super(topic);
        this.subscriptions = new ConcurrentHashMap<>();
        this.clientManageService = ClientManageService.ClientManageServiceEnum.INSTANCE.getInstance();
    }
}
