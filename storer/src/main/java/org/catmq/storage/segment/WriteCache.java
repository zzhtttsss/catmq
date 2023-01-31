package org.catmq.storage.segment;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.common.MessageEntry;
import org.catmq.common.MessageEntryBatch;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.catmq.storage.segment.SegmentStorage.MAX_CACHE_SIZE;

/**
 * Cache {@link MessageEntry} to improve performance.
 */
@Slf4j
public class WriteCache {
    /**
     * Represent the beginning offset of current {@link WriteCache}.
     */
    public static AtomicLong segmentOffset = new AtomicLong(0L);

    private long maxCacheSize;
    @Getter
    private final AtomicLong cacheSize;
    @Getter
    private AtomicInteger entryNum;

    private AtomicInteger appendingCount = new AtomicInteger(0);
    /**
     * Cache {@link MessageEntry} thread-safely and let us get a {@link MessageEntry} by its segmentId
     * and entryId.
     */
    @Getter
    private final ConcurrentHashMap<Long, Map<Long, MessageEntry>> cache = new ConcurrentHashMap<>();

    public WriteCache(long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
        this.cacheSize = new AtomicLong(0L);
        this.entryNum = new AtomicInteger(0);
    }

    /**
     * Append a {@link MessageEntry} to the cache.
     *
     * @param messageEntry {@link MessageEntry} need to be appended
     * @return whether succeed
     */
    public boolean appendEntry(MessageEntry messageEntry) {
        // If cache does not have enough space, deny the request.
        if (cacheSize.get() + messageEntry.getTotalSize() > maxCacheSize) {
            maxCacheSize = cacheSize.get();
            return false;
        }
        appendingCount.incrementAndGet();

        Map<Long, MessageEntry> map = cache.getOrDefault(messageEntry.getSegmentId(), null);
        if (map == null) {
            map = Collections.synchronizedMap(new LinkedHashMap<>());
            cache.put(messageEntry.getSegmentId(), map);
        }
        map.put(messageEntry.getEntryId(), messageEntry);
        cacheSize.addAndGet(messageEntry.getTotalSize());
        segmentOffset.addAndGet(messageEntry.getTotalSize());
        entryNum.incrementAndGet();
        appendingCount.decrementAndGet();
        return true;
    }

    public boolean batchAppendEntry(MessageEntryBatch messageEntryBatch) {
        // If cache does not have enough space, deny the request.
        if (cacheSize.get() + messageEntryBatch.getTotalSize() > maxCacheSize) {
            maxCacheSize = cacheSize.get();
            return false;
        }
        long totalSize = messageEntryBatch.getTotalSize();
        appendingCount.incrementAndGet();
        Map<Long, MessageEntry> map = cache.getOrDefault(messageEntryBatch.getBatchSegmentId(), null);
        if (map == null) {
            map = Collections.synchronizedMap(new LinkedHashMap<>());
            cache.put(messageEntryBatch.get(0).getSegmentId(), map);
        }
        for (MessageEntry me : messageEntryBatch.getBatch()) {
            map.put(me.getEntryId(), me);
        }
        cacheSize.addAndGet(totalSize);
        entryNum.addAndGet(messageEntryBatch.getBatch().size());
        segmentOffset.addAndGet(totalSize);
        appendingCount.decrementAndGet();
        return true;
    }

    public MessageEntry getEntry(long segmentId, Long entryId) {
        Map<Long, MessageEntry> map = this.cache.get(segmentId);
        if (map != null) {
            return map.getOrDefault(entryId, null);
        }
        return null;
    }

    public void clear() {
        cache.clear();
        cacheSize.set(0L);
        entryNum.set(0);
        maxCacheSize = MAX_CACHE_SIZE;
    }

    public boolean isEmpty() {
        return cacheSize.get() == 0L;
    }

    public boolean ready2Swap() {
        return appendingCount.get() == 0;
    }

}
