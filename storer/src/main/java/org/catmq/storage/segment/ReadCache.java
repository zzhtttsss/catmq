package org.catmq.storage.segment;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.common.MessageEntry;
import org.catmq.common.MessageEntryBatch;

import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Getter
public class ReadCache {

    private final long maxCacheSize;

    private final AtomicLong cacheSize;

    private final ConcurrentLinkedHashMap<SegmentBatchKey, ConcurrentLinkedHashMap<Long, MessageEntry>> cache;

    private final CleanReadCacheService cleanReadCacheService;


    /**
     * Put a {@link MessageEntry} into cache whose key are segmentId and entryId.
     *
     * @param messageEntry msg to be put
     */
    public void putEntry(MessageEntry messageEntry) {
        while (this.cacheSize.addAndGet(messageEntry.getTotalSize()) > maxCacheSize) {
            cleanReadCacheService.cleanCache(messageEntry.getTotalSize());
        }
        SegmentBatchKey key = new SegmentBatchKey(messageEntry.getSegmentId());
        key.addSize(messageEntry.getTotalSize());
        cache.computeIfAbsent(key, k -> new ConcurrentLinkedHashMap.Builder<Long, MessageEntry>()
                        .maximumWeightedCapacity(maxCacheSize)
                        .build())
                .put(messageEntry.getEntryId(), messageEntry);
    }

    public void putBatch(MessageEntryBatch batch) {
        while (this.cacheSize.addAndGet(batch.getTotalSize()) > maxCacheSize) {
            cleanReadCacheService.cleanCache(batch.getTotalSize());
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


    public ReadCache(long cacheSize) {
        this.maxCacheSize = cacheSize;
        this.cacheSize = new AtomicLong(0);
        this.cache = new ConcurrentLinkedHashMap.Builder<SegmentBatchKey,
                ConcurrentLinkedHashMap<Long, MessageEntry>>()
                .maximumWeightedCapacity(maxCacheSize)
                .build();
        this.cleanReadCacheService = new CleanReadCacheService(this);
        this.cleanReadCacheService.start();
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
