package org.catmq.broker.topic.persistent;

import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.common.Consumer;
import org.catmq.broker.manager.BrokerZkManager;
import org.catmq.broker.manager.ClientManager;
import org.catmq.broker.topic.BaseTopic;
import org.catmq.broker.topic.Subscription;
import org.catmq.broker.topic.Topic;
import org.catmq.entity.TopicDetail;
import org.catmq.protocol.definition.Code;
import org.catmq.protocol.definition.NumberedMessage;
import org.catmq.protocol.definition.OriginMessage;
import org.catmq.protocol.definition.Status;
import org.catmq.protocol.service.SendMessage2BrokerResponse;
import org.catmq.protocol.service.SendMessage2StorerResponse;
import org.catmq.zk.ZkIdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicStampedReference;

import static org.catmq.broker.Broker.BROKER;
import static org.catmq.entity.BrokerConfig.BROKER_CONFIG;

@Slf4j
public class PersistentTopic extends BaseTopic implements Topic {
    /**
     * Subscriptions to this topic whose key is subscriptionName
     */
    private final ConcurrentHashMap<String, PersistentSubscription> subscriptions;

    private final ClientManager clientManager;

    private Long lastAppendSegmentId;

    private final AtomicLong lastAppendEntryId;

    private long maxSegmentMessageNum;

    private String[] currentStorerAddresses;

    private final AtomicInteger runningCount = new AtomicInteger(0);

    private final AtomicStampedReference<Boolean> isSwitching = new AtomicStampedReference<>(false, 1);

    private final BrokerZkManager brokerZkManager;

    @Override
    public CompletableFuture<SendMessage2BrokerResponse> putMessage(List<OriginMessage> messages) {
        List<NumberedMessage> numberedMessages = new ArrayList<>();
        switch (getTopicDetail().getMode()) {
            case NORMAL -> {
                numberedMessages = allocateEntryIdThreadSafely(messages);
            }
            case ORDERED -> {
                numberedMessages = allocateEntryId(messages);
            }
            default -> {
                // TODO 上层会进行拦截，应该不可能进入这里
            }

        }

        CompletableFuture<SendMessage2BrokerResponse> future = BROKER.getStorerManager()
                .sendMessage2Storer(numberedMessages, super.getTopicDetail().getMode(), currentStorerAddresses)
                .thenApply(responses -> conv2SendMessage2BrokerResponse(responses));
        return future;
    }

    private List<NumberedMessage> allocateEntryId(List<OriginMessage> messages) {
        // TODO
        return allocateEntryIdThreadSafely(messages);
    }

    private List<NumberedMessage> allocateEntryIdThreadSafely(List<OriginMessage> messages) {
        int stamp = isSwitching.getStamp();
        List<NumberedMessage> numberedMessages = doAllocateEntryIdThreadSafely(messages);
        if (numberedMessages == null) {
            // Only one writer thread can swap the writeCache.
            if (isSwitching.compareAndSet(false, true, stamp, stamp + 1)) {
                log.warn("Cas success, start swapping.");
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    log.warn("Interrupted!", e);
                }
                while (!(runningCount.get() == 0)) {

                }
                switch2NextSegment();
            } else {
                log.warn("Cas fail, start waiting.");
                // Blocking if other writer thread is swapping the writeCache.
                while (isSwitching.getReference()) {

                }
            }
            // After swapping, try again.
            numberedMessages = doAllocateEntryIdThreadSafely(messages);
        }
        return numberedMessages;
    }


    private List<NumberedMessage> doAllocateEntryIdThreadSafely(List<OriginMessage> messages) {
        if (maxSegmentMessageNum < lastAppendEntryId.get() + messages.size()) {
            log.warn("no enough space");
            maxSegmentMessageNum = lastAppendEntryId.get();
            return null;
        }
        runningCount.incrementAndGet();
        long firstEntryId = lastAppendEntryId.getAndAdd(messages.size()) + 1;
        long segmentId = lastAppendSegmentId;
        runningCount.decrementAndGet();
        List<NumberedMessage> numberedMessages = new ArrayList<>(messages.size());
        for (OriginMessage om : messages) {
            numberedMessages.add(NumberedMessage.newBuilder()
                    .setBody(om.getBody())
                    .setEntryId(firstEntryId)
                    .setSegmentId(segmentId)
                    .build());
            firstEntryId++;
        }
        return numberedMessages;

    }

    private SendMessage2BrokerResponse conv2SendMessage2BrokerResponse(List<SendMessage2StorerResponse> responses) {
        SendMessage2BrokerResponse.Builder builder = SendMessage2BrokerResponse.newBuilder();
        for (SendMessage2StorerResponse response : responses) {
            if (response.getStatus().getCode() != Code.OK) {
                throw new RuntimeException("fail to send message to storer");
            }
        }
        builder.setStatus(Status.newBuilder().setCode(Code.OK).build())
                .setRes("success")
                .setAck(true);
        return builder.build();
    }

    private void switch2NextSegment() {
        // TODO 通知storer 惰性通知，storer在拿到一个entryId为0的消息时即判断是一个新segment.
        lastAppendSegmentId = ZkIdGenerator.ZkIdGeneratorEnum.INSTANCE.getInstance().nextId(BROKER.getClient());
        lastAppendEntryId.set(0);
        this.currentStorerAddresses = brokerZkManager.selectStorer(1).orElseThrow();
        this.maxSegmentMessageNum = BROKER_CONFIG.getMaxSegmentMessageNum();
        isSwitching.set(false, isSwitching.getStamp() + 1);
    }

    @Override
    public void subscribe(String subscriptionName, long consumerId) {
        log.info("[{}][{}] Created new subscription for {}", topicName, subscriptionName, consumerId);
        PersistentSubscription subscription = subscriptions.computeIfAbsent(subscriptionName,
                name -> new PersistentSubscription(this, subscriptionName));
        Consumer consumer = clientManager.getConsumer(consumerId);
        consumer.setSubscription(subscription);
        consumer.setTopicName(topicName);
        subscription.addConsumer(consumer);
    }

    @Override
    public Subscription getOrCreateSubscription(String subscriptionName) {
        log.info("[{}] topic created new subscription [{}]", topicName, subscriptionName);
        return this.subscriptions.computeIfAbsent(subscriptionName,
                name -> new PersistentSubscription(this, subscriptionName));
    }

    @Override
    public String getTopicName() {
        return super.topicName;
    }

    @Override
    public boolean isSubscribe(String subscriptionName, long consumerId) {
        PersistentSubscription subscription = subscriptions.get(subscriptionName);
        if (subscription == null) {
            return false;
        }
        return subscription
                .getDispatcher()
                .map(dispatcher -> dispatcher.isActiveConsumer(clientManager.getConsumer(consumerId)))
                .orElse(false);
    }


    public PersistentTopic(TopicDetail topicDetail, long segmentId) {
        super(topicDetail);
        this.brokerZkManager = BROKER.getBrokerZkManager();
        this.subscriptions = new ConcurrentHashMap<>();
        this.clientManager = BROKER.getClientManager();
        this.maxSegmentMessageNum = BROKER_CONFIG.getMaxSegmentMessageNum();
        this.lastAppendSegmentId = segmentId;
        this.lastAppendEntryId = new AtomicLong(0);
        this.currentStorerAddresses = brokerZkManager.selectStorer(1).orElseThrow();
    }
}
