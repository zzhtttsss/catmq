package org.catmq.broker.manager;

import com.google.common.annotations.VisibleForTesting;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.common.NumberedMessageBatch;
import org.catmq.broker.topic.persistent.PersistentTopic;
import org.catmq.protocol.definition.NumberedMessage;
import org.catmq.protocol.service.GetMessageFromStorerResponse;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static org.catmq.broker.Broker.BROKER;
import static org.catmq.entity.BrokerConfig.BROKER_CONFIG;

@Slf4j
@Getter
public class ReadCacheManager {

    private final long maxCacheSize;

    private final AtomicLong cacheSize;

    private final ReentrantLock lock;

    /**
     * <p>Cache for read message.The key is topic complete name and the value is a linked hash map
     * whose key is entryId and the value is {@link NumberedMessage}.</p>
     * <p>This cache is used to cache end-reading when producing new messages for the topic.</p>
     */
    private final ConcurrentLinkedHashMap<String, TopicCache> cache;


    public void putEntry(String completeName, NumberedMessage messageEntry) {
        if (!cache.containsKey(completeName)) {
            cache.put(completeName, new TopicCache());
        }
        putBatch(completeName, NumberedMessageBatch.of(messageEntry.getSegmentId(), messageEntry));
    }

    public void putBatch(String completeName, NumberedMessageBatch batch) {
        while (this.cacheSize.addAndGet(batch.getBatchSize()) > maxCacheSize) {
            log.warn("Read cache is full, clean the cache to make other messages can be put into cache");
            clean(completeName);
        }
        cache.computeIfAbsent(completeName, name -> new TopicCache()).putBatch(batch);
        log.info("Put batch into cache, topic: {}, batch size: {}", completeName, batch.getBatchSize());
    }

    public Optional<NumberedMessageBatch> getEntryBatch(String completeName, long segmentId, long entryId) {
        var topicCache = cache.get(completeName);
        if (topicCache == null) {
            return Optional.empty();
        }
        return topicCache.get(segmentId, entryId);
    }

    public Optional<NumberedMessageBatch> getEntryBatchFromStorer(String topicName, long segmentId, long entryId) {
        var topic = BROKER.getTopicManager().getTopic(topicName);
        if (topic instanceof PersistentTopic persistentTopic) {
            var addresses = persistentTopic.getCurrentStorerAddresses();
            //TODO use async ?
            try {
                GetMessageFromStorerResponse response = BROKER.getStorerManager()
                        .getMessageFromStorer(segmentId, entryId, addresses).get();
                return Optional.of(NumberedMessageBatch.of(response.getMessageList()));
            } catch (InterruptedException | ExecutionException e) {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    /**
     * Clean the cache to make other messages can be put into cache.
     * <p>When there is no more place to store messages, the clean function will be called.
     * It will clean the cache using LRU strategy</p>
     */
    @VisibleForTesting
    void clean(String targetTopic) {
        lock.lock();
        try {
            // double check
            if (cacheSize.get() <= maxCacheSize) {
                return;
            }
            // clean the oldest topic
            var iteratorTopic = cache.ascendingKeySet().iterator();
            while (iteratorTopic.hasNext()) {
                String topic = iteratorTopic.next();
                var topicCache = cache.get(topic);
                cacheSize.addAndGet(-topicCache.getTopicSize());
                iteratorTopic.remove();
                if (cacheSize.get() <= maxCacheSize) {
                    break;
                }
            }
            // clean the oldest segment batch
            var targetTopicCache = cache.get(targetTopic);
            var iteratorSegment = targetTopicCache.ascendingKeySet().iterator();
            while (iteratorSegment.hasNext()) {
                var key = iteratorSegment.next();
                var batch = targetTopicCache.get(key);
                cacheSize.addAndGet(-batch.getBatchSize());
                iteratorSegment.remove();
                if (cacheSize.get() <= maxCacheSize) {
                    break;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private ReadCacheManager() {
        this.maxCacheSize = BROKER_CONFIG.getMaxReadCacheSize();
        this.cacheSize = new AtomicLong(0);
        this.cache = new ConcurrentLinkedHashMap.Builder<String, TopicCache>()
                .maximumWeightedCapacity(maxCacheSize).build();
        this.lock = new ReentrantLock();
    }

    public enum ReadCacheManagerEnum {
        INSTANCE;

        private final ReadCacheManager readCacheManager;

        ReadCacheManagerEnum() {
            readCacheManager = new ReadCacheManager();
        }

        public ReadCacheManager getInstance() {
            return readCacheManager;
        }
    }
}


/**
 * <p>
 * SegmentBatchKey is used to identify a batch of messages in a segment.
 * It is the key in {@link TopicCache} and the value is {@link NumberedMessageBatch} which
 * stores the message batch.
 * </p>
 */
@Getter
class SegmentBatchKey implements Comparable<SegmentBatchKey> {
    private final long segmentId;
    private final long startEntryId;
    private final long endEntryId;

    private final int batchEntryNumber;

    public SegmentBatchKey(long segmentId, long startEntryId, long endEntryId) {
        this.segmentId = segmentId;
        this.startEntryId = startEntryId;
        this.endEntryId = endEntryId;
        this.batchEntryNumber = (int) (endEntryId - startEntryId + 1);
    }

    @Override
    public String toString() {
        return String.format("Segment %d[%d-%d] with size %d", segmentId, startEntryId,
                endEntryId, batchEntryNumber);
    }

    @Override
    public int compareTo(SegmentBatchKey o) {
        if (this.segmentId != o.segmentId) {
            return Long.compare(this.segmentId, o.segmentId);
        }
        return Long.compare(this.startEntryId, o.startEntryId);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(segmentId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj instanceof SegmentBatchKey other) {
            return this.compareTo(other) == 0;
        }
        return false;
    }
}

class TopicCache {
    private final ConcurrentLinkedHashMap<SegmentBatchKey, NumberedMessageBatch> cache;

    private final AtomicLong topicEntrySize;

    public TopicCache() {
        // TODO: use a better cache
        this.cache = new ConcurrentLinkedHashMap.Builder<SegmentBatchKey, NumberedMessageBatch>()
                .maximumWeightedCapacity(BROKER_CONFIG.getMaxReadCacheSize())
                .build();
        this.topicEntrySize = new AtomicLong(0);
    }

    public void putBatch(NumberedMessageBatch batch) {
        // return if the batch is empty
        if (batch.getBatch().size() == 0) {
            return;
        }
        long startEntryId = batch.getBatch().get(0).getEntryId();
        long endEntryId = batch.getBatch().size() + startEntryId - 1;
        var key = new SegmentBatchKey(batch.getSegmentId(), startEntryId, endEntryId);
        cache.put(key, batch);
        topicEntrySize.addAndGet(batch.getBatchSize());
    }

    public Optional<NumberedMessageBatch> get(long segmentId, long entryId) {
        for (var key : cache.keySet()) {
            if (key.getSegmentId() == segmentId && key.getStartEntryId() <= entryId &&
                    key.getEndEntryId() >= entryId) {
                return Optional.ofNullable(cache.get(key));
            }
        }
        return Optional.empty();
    }

    NumberedMessageBatch get(SegmentBatchKey key) {
        return cache.get(key);
    }

    public long getTopicSize() {
        return topicEntrySize.get();
    }

    public Set<SegmentBatchKey> ascendingKeySet() {
        return cache.ascendingKeySet();
    }
}
