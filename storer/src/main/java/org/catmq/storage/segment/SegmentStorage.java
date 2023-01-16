package org.catmq.storage.segment;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.storage.messageLog.MessageEntry;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.locks.ReentrantLock;

import static org.catmq.constant.FileConstant.MB;
import static org.catmq.storer.StorerConfig.STORER_CONFIG;

@Slf4j
public class SegmentStorage {

    @Getter
    private final String path;
    @Getter
    private final EntryOffsetIndex entryOffsetIndex;
    private final FlushWriteCacheService flushWriteCacheService;
//    public static final long MAX_CACHE_SIZE = (long) (0.25 * PlatformDependent.estimateMaxDirectMemory());
    public static final long MAX_CACHE_SIZE = STORER_CONFIG.getSegmentMaxFileSize();
    @Getter
    private WriteCache writeCache4Append;
    @Getter
    private WriteCache writeCache4Flush;
    @Getter
    private ReadCache readCache;
    protected final ReentrantLock flushLock = new ReentrantLock();
    private final AtomicStampedReference<Boolean> canSwap = new AtomicStampedReference<>(false, 1);
    @Getter
    private final CopyOnWriteArrayList<Segment> segments;

    public SegmentStorage() {
        this.path = STORER_CONFIG.getSegmentStoragePath();
        this.writeCache4Append = new WriteCache(MAX_CACHE_SIZE);
        this.writeCache4Flush = new WriteCache(MAX_CACHE_SIZE);
        this.readCache = new ReadCache();
        this.segments = new CopyOnWriteArrayList<>();
        this.flushWriteCacheService = new FlushWriteCacheService(this);
        entryOffsetIndex = new EntryOffsetIndex(KeyValueStorageRocksDB.factory,
                STORER_CONFIG.getSegmentIndexStoragePath());
        flushWriteCacheService.start();
    }

    public void appendEntry2WriteCache(MessageEntry messageEntry) {
        int stamp = canSwap.getStamp();
        boolean ok = writeCache4Append.appendEntry(messageEntry);
        if (!ok) {
            // Only one thread can swap the cache.
            if (canSwap.compareAndSet(false, true, stamp, stamp + 1)) {
                log.warn("Cas success, start swapping.");
                swapAndFlush();
            }
            else {
                log.warn("Cas fail, start waiting.");
                while (canSwap.getReference()) {
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
        WriteCache temp = this.writeCache4Append;
        this.writeCache4Append = this.writeCache4Flush;
        canSwap.set(false, canSwap.getStamp() + 1);
        this.writeCache4Flush = temp;
        flushLock.unlock();
    }

    public void clearFlushedCache() {
        writeCache4Flush.clear();
    }

}
