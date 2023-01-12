package org.catmq.storage.segment;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.storage.messageLog.MessageEntry;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;

@Slf4j
public class PartitionSegmentStorage {

    @Getter
    private final String path;

    @Getter
    private final EntryOffsetIndex entryOffsetIndex;

    private final FlushWriteCacheService flushWriteCacheService;

    public static final long MAX_CACHE_SIZE = (long) (0.25 * PlatformDependent.estimateMaxDirectMemory());

    @Getter
    private WriteCache writeCache4Append;

    @Getter
    private WriteCache writeCache4Flush;

    @Getter
    private ReadCache readCache;

    private final StampedLock writeCacheRotationLock = new StampedLock();

    protected final ReentrantLock flushLock = new ReentrantLock();

    private AtomicBoolean swaping = new AtomicBoolean(false);

    @Getter
    private final CopyOnWriteArrayList<PartitionSegment> partitionSegments;

    public PartitionSegmentStorage() {
        this.path = "./segment/";
        this.writeCache4Append = new WriteCache(MAX_CACHE_SIZE);
        this.writeCache4Flush = new WriteCache(MAX_CACHE_SIZE);
        this.readCache = new ReadCache();
        this.partitionSegments = new CopyOnWriteArrayList<>();
        this.flushWriteCacheService = new FlushWriteCacheService(this);
        entryOffsetIndex = new EntryOffsetIndex(KeyValueStorageRocksDB.factory, "");
    }

    public void appendEntry2WriteCache(MessageEntry messageEntry) {
        long stamp = writeCacheRotationLock.writeLock();
        boolean ok = writeCache4Append.appendEntry(messageEntry);
        if (!ok) {
            // Only one thread can swap the cache.
            if (swaping.compareAndSet(false, true)) {
                swapAndFlush();
            }
            else {
                while (swaping.get()) {
                    // Blocking if other thread is swapping the cache.
                }
            }
            this.writeCache4Append.appendEntry(messageEntry);
        }
        writeCacheRotationLock.unlockWrite(stamp);
    }

    public void dumpEntry2Index() {
        this.writeCache4Flush.getCache().forEach((segmentId, map) -> {
            map.forEach((msgId, messageEntry) -> {
//                entryOffsetIndex.addLocation(segmentId, msgId, );
            });
        });
    }

    public void swapAndFlush() {
        swapWriteCache();
        flushWriteCacheService.getRequestQueue().add(writeCache4Flush);
    }

    public void swapWriteCache() {
        flushLock.lock();
        WriteCache temp = this.writeCache4Append;
        this.writeCache4Append = this.writeCache4Flush;
        swaping.compareAndSet(true, false);
        this.writeCache4Flush = temp;
        flushLock.unlock();
    }

    public void clearFlushedCache() {
        writeCache4Flush.clear();
    }

}
