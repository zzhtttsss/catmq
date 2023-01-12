package org.catmq.storage.segment;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.catmq.storage.messageLog.MessageEntry;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class WriteCache {

    public final long maxCacheSize;

    @Getter
    private AtomicLong cacheSize;

    @Getter
    private AtomicLong offset;

    @Getter
    private AtomicInteger entryNum;

    @Getter
    private final ConcurrentHashMap<Long, LinkedHashMap<Long, MessageEntry>> cache = new ConcurrentHashMap<>();

    public WriteCache(long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
        this.cacheSize = new AtomicLong(0L);
        this.offset = new AtomicLong(0L);
        this.entryNum = new AtomicInteger(0);
    }

    public boolean appendEntry(MessageEntry messageEntry) {
        if (cacheSize.get() + messageEntry.getTotalSize() > maxCacheSize) {
            return false;
        }
        cache.getOrDefault(messageEntry.getSegmentId(), new LinkedHashMap<>()).put(messageEntry.getEntryId(), messageEntry);
        cacheSize.addAndGet(messageEntry.getTotalSize());
        messageEntry.setOffset(offset.get());
        offset.addAndGet(messageEntry.getTotalSize());
        entryNum.incrementAndGet();
        return true;
    }

    public MessageEntry getEntry(long chunkId, String msgId) {
        LinkedHashMap<Long, MessageEntry> map = this.cache.get(chunkId);
        if (map != null) {
            return map.get(msgId);
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
