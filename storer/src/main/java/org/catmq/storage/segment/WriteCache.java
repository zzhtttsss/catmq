package org.catmq.storage.segment;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.catmq.storage.messageLog.MessageEntry;

import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class WriteCache {

    public final long maxCacheSize;

    @Getter
    private AtomicLong cacheSize;

    @Getter
    private AtomicInteger entryNum;

    @Getter
    private final ConcurrentHashMap<Long, LinkedHashMap<String, MessageEntry>> cache = new ConcurrentHashMap<>();

    public WriteCache(long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
        this.cacheSize = new AtomicLong(0L);
        this.entryNum = new AtomicInteger(0);
    }

    /*
    * There may cause some overflow of the cache depended on write threads num, but it is ok.
    * */
    public boolean appendEntry(MessageEntry messageEntry) {
        if (cacheSize.get() + messageEntry.getTotalSize() > maxCacheSize) {
            return false;
        }

        cache.getOrDefault(messageEntry.getSegmentId(), new LinkedHashMap<>()).put(messageEntry.getMsgId(), messageEntry);
        cacheSize.addAndGet(messageEntry.getTotalSize());
        entryNum.incrementAndGet();
        return true;
    }

    public MessageEntry getEntry(long chunkId, String msgId) {
        LinkedHashMap<String, MessageEntry> map = this.cache.get(chunkId);
        if (map != null) {
            return map.get(msgId);
        }
        return null;
    }

    public void dumpEntry2ByteBuf(ByteBuf byteBuf) {
        this.cache.forEach((segmentId, map) -> {
            map.forEach((msgId, messageEntry) -> {
                byteBuf.writeInt(messageEntry.getLength());
                byteBuf.writeBytes(messageEntry.getMessage());
            });
        });
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
