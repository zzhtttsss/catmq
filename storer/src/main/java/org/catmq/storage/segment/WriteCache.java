package org.catmq.storage.segment;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.storage.messageLog.MessageEntry;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class WriteCache {

    public final long maxCacheSize;

    @Getter
    private final AtomicLong cacheSize;

    @Getter
    public static AtomicLong segmentOffset = new AtomicLong(0L);

    @Getter
    private AtomicInteger entryNum;

    @Getter
    private final ConcurrentHashMap<Long, Map<Long, MessageEntry>> cache = new ConcurrentHashMap<>();

    public WriteCache(long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
        this.cacheSize = new AtomicLong(0L);
        this.entryNum = new AtomicInteger(0);
    }

    public boolean appendEntry(MessageEntry messageEntry) {
        if (cacheSize.get() + messageEntry.getTotalSize() > maxCacheSize) {
            return false;
        }

        Map<Long, MessageEntry> map = cache.getOrDefault(messageEntry.getSegmentId(), null);
        if (map == null) {
            map = Collections.synchronizedMap(new LinkedHashMap<>());
            cache.put(messageEntry.getSegmentId(), map);
        }
        map.put(messageEntry.getEntryId(), messageEntry);

        cacheSize.addAndGet(messageEntry.getTotalSize());
        messageEntry.setOffset(segmentOffset.get());
        segmentOffset.addAndGet(messageEntry.getTotalSize());
        entryNum.incrementAndGet();
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
    }

    public boolean isEmpty() {
        return cacheSize.get() == 0L;
    }

}
