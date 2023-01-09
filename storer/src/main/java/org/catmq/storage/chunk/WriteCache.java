package org.catmq.storage.chunk;

import io.netty.util.internal.PlatformDependent;
import lombok.Getter;
import org.catmq.storage.messageLog.MessageEntry;

import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentHashMap;

import static org.catmq.constant.FileConstant.MB;
import static org.catmq.storage.messageLog.MessageLog.LENGTH_OF_INT;

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

    public void addEntry(MessageEntry messageEntry) {
        if (cacheSize + messageEntry.getTotalSize() > maxCacheSize) {
            return;
        }

        cache.getOrDefault(messageEntry.getChunkId(), new LinkedHashMap<>()).put(messageEntry.getMsgId(), messageEntry);
        cacheSize += messageEntry.getTotalSize();
        entryNum++;
    }

    public MessageEntry getEntry(long chunkId, String msgId) {
        LinkedHashMap<String, MessageEntry> map = this.cache.get(chunkId);
        if (map != null) {
            return map.get(msgId);
        }
        return null;
    }


}
