package org.catmq.broker.topic.persistent;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.common.Consumer;
import org.catmq.broker.common.NumberedMessageBatch;
import org.catmq.broker.manager.BrokerZkManager;
import org.catmq.broker.manager.ClientManager;
import org.catmq.broker.manager.TopicManager;
import org.catmq.broker.topic.BaseTopic;
import org.catmq.broker.topic.Subscription;
import org.catmq.broker.topic.Topic;
import org.catmq.entity.TopicDetail;
import org.catmq.entity.TopicMode;
import org.catmq.protocol.definition.Code;
import org.catmq.protocol.definition.NumberedMessage;
import org.catmq.protocol.definition.OriginMessage;
import org.catmq.protocol.definition.Status;
import org.catmq.protocol.service.SendMessage2BrokerResponse;
import org.catmq.protocol.service.SendMessage2StorerResponse;
import org.catmq.zk.ZkIdGenerator;

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

    private final TopicManager topicManager;

    private Long lastAppendSegmentId;

    private final AtomicLong lastAppendEntryId;

    private long maxSegmentMessageNum;

    /**
     * The address of storer which are responsible for storing messages of the current segment.
     */
    @Getter
    private String[] currentStorerAddresses;

    /**
     * The count of threads which are allocating entry id and segment id to a batch of messages.
     */
    private final AtomicInteger runningCount = new AtomicInteger(0);

    /**
     * The flag of switching to next segment to make sure only one thread can switch topic to next segment.
     */
    private final AtomicStampedReference<Boolean> isSwitching = new AtomicStampedReference<>(false, 1);

    private final BrokerZkManager brokerZkManager;

    private final boolean threadSafe;

    @Override
    public CompletableFuture<SendMessage2BrokerResponse> putMessage(List<OriginMessage> messages) {
        NumberedMessageBatch numberedMessages;
        switch (getTopicDetail().getMode()) {
            // For normal topic, SendMessage2BrokerRequest will be handled randomly by thread-pool,
            // it means there are concurrency, so we need to allocate entry id thread safely.For
            // ordered topic, all SendMessage2BrokerRequest of a same topic will be handled by one
            // thread, so we don't need to care about concurrency.
            case NORMAL -> {
                numberedMessages = allocateEntryIdThreadSafely(messages);
            }
            case ORDERED -> {
                numberedMessages = allocateEntryId(messages);
            }
            default -> {
                throw new RuntimeException("Unsupported topic mode.");
            }

        }
        return BROKER.getStorerManager()
                .sendMessage2Storer(numberedMessages, super.getTopicDetail().getMode(), currentStorerAddresses)
                .thenApply(responses -> {
                    var response = conv2SendMessage2BrokerResponse(responses);
                    if (response.getStatus().getCode() == Code.OK) {
                        // If the message is successfully stored, update the number of segment.
                        long segmentId = numberedMessages.getSegmentId();
                        long num = numberedMessages.getBatch().size();
                        topicManager.addSegmentNumber(segmentId, num);
                        topicManager.addSegmentId(topicName, segmentId);
                        BROKER.getReadCacheManager().putBatch(topicName, numberedMessages);
                    }
                    return response;
                });
    }

    private NumberedMessageBatch allocateEntryId(List<OriginMessage> messages) {
        NumberedMessageBatch numberedMessages = doAllocateEntryId(messages);
        if (numberedMessages == null) {
            switch2NextSegment();
            numberedMessages = doAllocateEntryId(messages);
        }
        return numberedMessages;
    }

    /**
     * Allocate entry id and segment id for a batch of messages thread safely without lock.
     * <p>When the current segment doesn't have enough space, we need to switch this topic to next
     * segment. There must be no other thread allocating entry id and segment id (read and write
     * lastAppendSegmentId and lastAppendEntryId) when switching. So we use a counter to count the
     * number of threads which are allocating entry id and segment id. The switching thread will
     * wait until the counter is 0. But there still exists a problem：
     * <p> Assuming that the current counter is zero, the switching thread A just runs to the
     * statement that judges that the counter is zero (!(runningCount.get() == 0)), and
     * the other thread B executes the statement that increases the counter, and A completes
     * the judgment before B increases the counter, so that as a result, B can allocate
     * entry id and segment id when switching.
     * <p> To solve this problem, A will wait for 0.01ms before judging the counter to ensure that B
     * has enough time to increase the counter.
     *
     * @param messages batch of OriginMessage
     * @return batch of NumberedMessage
     */
    private NumberedMessageBatch allocateEntryIdThreadSafely(List<OriginMessage> messages) {
        // If other thread is switching, start waiting.
        while (isSwitching.getReference()) {

        }
        // If current segment has enough space, allocate entry id and segment id directly.
        int stamp = isSwitching.getStamp();
        NumberedMessageBatch numberedMessages = doAllocateEntryId(messages);
        // If current segment doesn't have enough space, try to cas to do switching.
        if (numberedMessages == null) {
            if (isSwitching.compareAndSet(false, true, stamp, stamp + 1)) {
                // CAS success, start switching.
                log.info("Cas success, start switching.");
                // Wait until there is no other thread allocating entry id and segment id.
                long current = System.nanoTime();
                while (System.nanoTime() - current < 10000 || runningCount.get() != 0) {

                }
                // Switch this topic to next segment.
                switch2NextSegment();
            } else {
                // CAS fail, waiting until other thread finishes switching.
                log.info("Cas fail, start waiting.");
                while (isSwitching.getReference()) {

                }
            }
            // Try to allocate entry id and segment id again.
            // Because the number of threads to wait for switching is much less than the number of
            // a segment can hold, so we don't need to worry about this time the current segment
            // doesn't have enough space.
            numberedMessages = doAllocateEntryId(messages);
        }
        return numberedMessages;
    }


    private NumberedMessageBatch doAllocateEntryId(List<OriginMessage> messages) {
        // If current segment doesn't have enough space, return null.
        if (maxSegmentMessageNum < lastAppendEntryId.get() + messages.size()) {
            log.info("Segment is full,segment id: {} last entry id: {}", lastAppendSegmentId, lastAppendEntryId.get());
            maxSegmentMessageNum = lastAppendEntryId.get();
            return null;
        }
        // If we need to ensure thread safety, count.
        if (threadSafe) {
            runningCount.incrementAndGet();
        }
        long firstEntryId = lastAppendEntryId.getAndAdd(messages.size()) + 1;
        long segmentId = lastAppendSegmentId;
        if (threadSafe) {
            runningCount.decrementAndGet();
        }
        NumberedMessageBatch numberedMessages = new NumberedMessageBatch(segmentId);
        for (OriginMessage om : messages) {
            numberedMessages.addMessage(NumberedMessage.newBuilder()
                    .setBody(om.getBody())
                    .setEntryId(firstEntryId)
                    .setSegmentId(segmentId)
                    .build());
            // Call addSegmentEntrySize here make sure these entries is successfully stored.
            // TODO: entryId may be not sequential because of the failure of some entries.
            // topicManager.addSegmentEntrySize(segmentId, firstEntryId);
            firstEntryId++;
        }
        return numberedMessages;
    }

    private SendMessage2BrokerResponse conv2SendMessage2BrokerResponse(List<SendMessage2StorerResponse> responses) {
        SendMessage2BrokerResponse.Builder builder = SendMessage2BrokerResponse.newBuilder();
        // TODO: handle response
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
        lastAppendSegmentId = ZkIdGenerator.ZkIdGeneratorEnum.INSTANCE.getInstance().nextId(BROKER.getClient());
        topicManager.addSegmentId(topicName, lastAppendSegmentId);
        lastAppendEntryId.set(0);
        this.currentStorerAddresses = brokerZkManager.selectStorer(1).orElseThrow();
        this.maxSegmentMessageNum = BROKER_CONFIG.getMaxSegmentMessageNum();
        if (threadSafe) {
            isSwitching.set(false, isSwitching.getStamp() + 1);
        }
    }

    @Override
    public void subscribe(String subscriptionName, long consumerId) {
        log.info("[{}][{}] Created new subscription for {}", consumerId, subscriptionName, topicName);
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
        this.topicManager = BROKER.getTopicManager();
        this.maxSegmentMessageNum = BROKER_CONFIG.getMaxSegmentMessageNum();
        this.lastAppendSegmentId = segmentId;
        this.lastAppendEntryId = new AtomicLong(0);
        this.currentStorerAddresses = brokerZkManager.selectStorer(1).orElseThrow();
        this.threadSafe = super.getTopicDetail().getMode() == TopicMode.NORMAL;
    }
}
