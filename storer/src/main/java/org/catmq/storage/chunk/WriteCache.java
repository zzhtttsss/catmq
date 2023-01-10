package org.catmq.storage.chunk;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import org.catmq.storage.messageLog.MessageEntry;

import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;

public class WriteCache {

    public final long maxCacheSize;

    @Getter
    private long cacheSize;

    @Getter
    private int entryNum;

    @Getter
    private final ConcurrentHashMap<Long, LinkedHashMap<String, MessageEntry>> cache = new ConcurrentHashMap<>();

    public WriteCache(long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
        this.cacheSize = 0;
        this.entryNum = 0;
    }

    public boolean appendEntry(MessageEntry messageEntry) {
        if (cacheSize + messageEntry.getTotalSize() > maxCacheSize) {
            return false;
        }

        cache.getOrDefault(messageEntry.getSegmentId(), new LinkedHashMap<>()).put(messageEntry.getMsgId(), messageEntry);
        cacheSize += messageEntry.getTotalSize();
        entryNum++;
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


}
