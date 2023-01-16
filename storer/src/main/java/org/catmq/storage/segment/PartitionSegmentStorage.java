package org.catmq.storage.segment;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.storage.messageLog.MessageEntry;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.locks.ReentrantLock;

import static org.catmq.constant.FileConstant.MB;

@Slf4j
public class PartitionSegmentStorage {

    @Getter
    private final String path;

    @Getter
    private final EntryOffsetIndex entryOffsetIndex;

    private final FlushWriteCacheService flushWriteCacheService;

//    public static final long MAX_CACHE_SIZE = (long) (0.25 * PlatformDependent.estimateMaxDirectMemory());
    public static final long MAX_CACHE_SIZE = 1 * MB;

    @Getter
    private WriteCache writeCache4Append;

    @Getter
    private WriteCache writeCache4Flush;

    @Getter
    private ReadCache readCache;

//    private final StampedLock writeCacheRotationLock = new StampedLock();

    protected final ReentrantLock flushLock = new ReentrantLock();

    private final AtomicBoolean swapping = new AtomicBoolean(false);
    private final AtomicStampedReference<Boolean> canSwap = new AtomicStampedReference<>(false, 1);

    @Getter
    private final CopyOnWriteArrayList<PartitionSegment> partitionSegments;

    public PartitionSegmentStorage() {
        this.path = "/Users/zzh/Documents/projects/catmq/catmq/storer/src/segment";
        this.writeCache4Append = new WriteCache(MAX_CACHE_SIZE);
        this.writeCache4Flush = new WriteCache(MAX_CACHE_SIZE);
        this.readCache = new ReadCache();
        this.partitionSegments = new CopyOnWriteArrayList<>();
        this.flushWriteCacheService = new FlushWriteCacheService(this);
        entryOffsetIndex = new EntryOffsetIndex(KeyValueStorageRocksDB.factory,
                "/Users/zzh/Documents/projects/catmq/catmq/storer/src/index");
        flushWriteCacheService.start();
    }

    public void appendEntry2WriteCache(MessageEntry messageEntry) {
        int stamp = canSwap.getStamp();
        boolean ok = writeCache4Append.appendEntry(messageEntry);
        if (!ok) {
            // Only one thread can swap the cache.
            if (canSwap.compareAndSet(false, true, stamp, stamp + 1)) {
                log.warn("cas success, start swapping");
                swapAndFlush();
            }
            else {
                log.warn("cas fail, start waiting");
                while (swapping.get()) {
                    // Blocking if other thread is swapping the cache.
                }
            }
            this.writeCache4Append.appendEntry(messageEntry);
        }
    }

    public void swapAndFlush() {
        swapWriteCache();
        flushWriteCacheService.getRequestQueue().add(writeCache4Flush);
    }

    public void swapWriteCache() {
        flushLock.lock();
        log.warn("writeCache4Append map size is {}", writeCache4Append.getCache().size());
        WriteCache temp = this.writeCache4Append;
        this.writeCache4Append = this.writeCache4Flush;
        canSwap.set(false, canSwap.getStamp() + 1);
        this.writeCache4Flush = temp;
        log.warn("writeCache4Flush map size is {}", writeCache4Flush.getCache().size());
        flushLock.unlock();
    }

    public void clearFlushedCache() {
        writeCache4Flush.clear();
    }

}
