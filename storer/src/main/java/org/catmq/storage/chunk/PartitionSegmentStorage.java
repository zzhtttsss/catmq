package org.catmq.storage.chunk;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.storage.messageLog.MessageEntry;

import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
public class PartitionSegmentStorage {

    @Getter
    private final String path;

    private final FlushWriteCacheService flushWriteCacheService;

    public static final long MAX_CACHE_SIZE = (long) (0.25 * PlatformDependent.estimateMaxDirectMemory());

    @Getter
    private WriteCache writeCache4Append;

    @Getter
    private WriteCache writeCache4Flush;

    @Getter
    private ReadCache readCache;

    @Getter
    private final CopyOnWriteArrayList<PartitionSegment> partitionSegments;

//    private CopyOnWriteArrayList<>

    public PartitionSegmentStorage() {
        this.path = "./segment/";
        this.writeCache4Append = new WriteCache(MAX_CACHE_SIZE);
        this.writeCache4Flush = new WriteCache(MAX_CACHE_SIZE);
        this.readCache = new ReadCache();
        this.partitionSegments = new CopyOnWriteArrayList<>();
        this.flushWriteCacheService = new FlushWriteCacheService(this);
    }

    public void appendEntry2WriteCache(MessageEntry messageEntry) {
        boolean ok = this.writeCache4Append.appendEntry(messageEntry);
        if (!ok) {
            swapAndFlush();
        }
        this.writeCache4Append.appendEntry(messageEntry);
    }

    public void swapAndFlush() {
        swapWriteCache();
        ByteBuf byteBuf = Unpooled.directBuffer();
        writeCache4Flush.dumpEntry2ByteBuf(byteBuf);
        flushWriteCacheService.getRequestQueue().add(byteBuf);
    }

    public void swapWriteCache() {
        WriteCache temp = this.writeCache4Append;
        this.writeCache4Append = this.writeCache4Flush;
        this.writeCache4Flush = temp;
    }

}
