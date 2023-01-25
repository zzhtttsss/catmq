package org.catmq.storage.segment;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.common.MessageEntry;
import org.catmq.common.MessageEntryBatch;
import org.catmq.thread.ServiceThread;

import java.util.concurrent.atomic.AtomicLong;

import static org.catmq.storer.StorerConfig.STORER_CONFIG;

@Slf4j
public class ReadCache extends ServiceThread {

    private final long maxCacheSize;

    @Getter
    private AtomicLong cacheSize;

    @Getter
    private final ConcurrentLinkedHashMap<SegmentBatchKey, ConcurrentLinkedHashMap<Long, MessageEntry>> cache;


    /**
     * Put a {@link MessageEntry} into cache whose key are segmentId and entryId.
     *
     * @param messageEntry msg to be put
     */
    public void putEntry(MessageEntry messageEntry) {
        if (this.cacheSize.addAndGet(messageEntry.getTotalSize()) > maxCacheSize) {
            this.cleanCache(messageEntry.getTotalSize());
        }
        SegmentBatchKey key = new SegmentBatchKey(messageEntry.getSegmentId());
        key.addSize(messageEntry.getTotalSize());
        cache.computeIfAbsent(key, k -> new ConcurrentLinkedHashMap.Builder<Long, MessageEntry>()
                        .maximumWeightedCapacity(maxCacheSize)
                        .build())
                .put(messageEntry.getEntryId(), messageEntry);
    }

    public void putBatch(MessageEntryBatch batch) {
        if (this.cacheSize.addAndGet(batch.getTotalSize()) > maxCacheSize) {
            log.info("ReadCache is full when putting {} with size {}.", batch.getBatchSegmentId(), batch.getTotalSize());
            this.cleanCache(batch.getTotalSize());
        }
        SegmentBatchKey key = new SegmentBatchKey(batch.getBatchSegmentId());
        for (MessageEntry messageEntry : batch.getBatch()) {
            cache.computeIfAbsent(key, k -> new ConcurrentLinkedHashMap.Builder<Long, MessageEntry>()
                            .maximumWeightedCapacity(maxCacheSize)
                            .build())
                    .put(messageEntry.getEntryId(), messageEntry);
            key.addSize(messageEntry.getTotalSize());
        }
    }

    public MessageEntry getEntry(long segmentId, long entryId) {
        var cacheSegment = cache.get(new SegmentBatchKey(segmentId));
        if (cacheSegment == null) {
            return null;
        }
        return cacheSegment.get(entryId);
    }

    @Override
    public String getServiceName() {
        return this.getClass().getName();
    }

    @Override
    public void run() {
        long lastFlushTime = System.currentTimeMillis();
        while (!this.isStopped()) {
            // Big Clean-up occurs when time is up or there are too many batch in queue.
            if (System.currentTimeMillis() - lastFlushTime > STORER_CONFIG.getReadCacheCleanUpInterval()) {
                this.cleanCache(-1);
                log.info("{} has cleaned up.", this.getServiceName());
                lastFlushTime = System.currentTimeMillis();
            }
        }
    }

    private void cleanCache(long batchSize) {
        if (batchSize < 0) {
            // Big clean-up occurs
            // Clean up an expired or deletable key.
            final long threshold = (long) (maxCacheSize * STORER_CONFIG.getReadCacheRemainingThreshold());
            cleanup0(threshold);
        } else {
            // double check
            if (cacheSize.get() > maxCacheSize) {
                cleanup0(maxCacheSize);
            }

        }
    }

    private synchronized void cleanup0(long threshold) {
        // Just clean up the oldest segment with its entries.
        for (var key : cache.ascendingKeySet()) {
            cacheSize.addAndGet(-key.getBatchSegmentSize());
            cache.remove(key);
            if (cacheSize.get() <= threshold) {
                return;
            }
        }
        // If the cache is still full, delete the oldest.
        var iterator = cache.ascendingKeySet().iterator();
        while (cacheSize.get() > threshold && iterator.hasNext()) {
            var key = iterator.next();
            cacheSize.addAndGet(-key.getBatchSegmentSize());
            cache.remove(key);
        }
        if (cacheSize.get() > threshold) {
            log.error("ReadCache is still full after cleaning up.");
        }
    }

    public ReadCache(long cacheSize) {
        this.maxCacheSize = cacheSize;
        this.cacheSize = new AtomicLong(0);
        this.cache = new ConcurrentLinkedHashMap.Builder<SegmentBatchKey,
                ConcurrentLinkedHashMap<Long, MessageEntry>>()
                .maximumWeightedCapacity(maxCacheSize)
                .build();

    }
}


class SegmentBatchKey implements Comparable<SegmentBatchKey> {
    @Getter
    private final long segmentId;
    @Getter
    private long batchSegmentSize;

    public SegmentBatchKey(long segmentId) {
        this.segmentId = segmentId;
        this.batchSegmentSize = 0;
    }

    public void addSize(long size) {
        this.batchSegmentSize += size;
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
        if (obj instanceof SegmentBatchKey) {
            return ((SegmentBatchKey) obj).segmentId == this.segmentId;
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("Segment %d with size %d", segmentId, batchSegmentSize);
    }

    @Override
    public int compareTo(SegmentBatchKey o) {
        return Long.compare(this.segmentId, o.segmentId);
    }
}
