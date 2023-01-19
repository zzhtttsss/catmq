package org.catmq.storage.segment;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.storage.MessageEntry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.locks.ReentrantLock;

import static org.catmq.storer.StorerConfig.STORER_CONFIG;

/**
 * Manage all segment.
 */
@Slf4j
public class SegmentStorage {

    @Getter
    private final String path;
    @Getter
    private final EntryPositionIndex entryPositionIndex;
    private final FlushWriteCacheService flushWriteCacheService;
    public static final long MAX_CACHE_SIZE = STORER_CONFIG.getSegmentMaxFileSize();
    /**
     * Cache all incoming {@link MessageEntry}
     */
    @Getter
    private WriteCache writeCache4Append;
    /**
     * Cache all {@link MessageEntry} to be flushed to the disk.
     */
    @Getter
    private WriteCache writeCache4Flush;
    @Getter
    private ReadCache readCache;
    /**
     * Make sure that swapping {@link WriteCache} and flushing the cache to disk don't happen at
     * the same time.
     */
    protected final ReentrantLock flushLock = new ReentrantLock();
    /**
     * Make sure that only one writer thread can {@link WriteCache} each time {@link WriteCache}
     * is full. We use {@link AtomicStampedReference} to prevent the ABA problem.
     */
    private final AtomicStampedReference<Boolean> canSwap = new AtomicStampedReference<>(false, 1);
    @Getter
    private final ConcurrentHashMap<Long, Segment> segments;

    public SegmentStorage() {
        this.path = STORER_CONFIG.getSegmentStoragePath();
        this.writeCache4Append = new WriteCache(MAX_CACHE_SIZE);
        this.writeCache4Flush = new WriteCache(MAX_CACHE_SIZE);
        this.readCache = new ReadCache();
        segments = new ConcurrentHashMap<>();
        this.flushWriteCacheService = new FlushWriteCacheService(this);
        entryPositionIndex = new EntryPositionIndex(KeyValueStorageRocksDB.factory,
                STORER_CONFIG.getSegmentIndexStoragePath());
        flushWriteCacheService.start();
    }

    /**
     * Append an {@link MessageEntry} to the writeCache4Append.
     *
     * @param messageEntry message entry need to be appended to writeCache4Append
     */
    public void appendEntry2WriteCache(MessageEntry messageEntry) {
        int stamp = canSwap.getStamp();
        boolean ok = writeCache4Append.appendEntry(messageEntry);
        if (!ok) {
            // Only one writer thread can swap the writeCache.
            if (canSwap.compareAndSet(false, true, stamp, stamp + 1)) {
                log.warn("Cas success, start swapping.");
                swapAndFlush();
            }
            else {
                log.warn("Cas fail, start waiting.");
                // Blocking if other writer thread is swapping the writeCache.
                while (canSwap.getReference()) {

                }
            }
            // After swapping, try again.
            this.writeCache4Append.appendEntry(messageEntry);
        }
    }

    /**
     * Swap two {@link WriteCache} (writeCache4Append and writeCache4Flush) and let
     * {@link FlushWriteCacheService} to flush writeCache4Flush.
     */
    public void swapAndFlush() {
        swapWriteCache();
        flushWriteCacheService.getRequestQueue().add(writeCache4Flush);
    }

    public void swapWriteCache() {
        flushLock.lock();
        WriteCache temp = this.writeCache4Append;
        this.writeCache4Append = this.writeCache4Flush;
        // When writeCache4Append finishes swapping, immediately set canSwap to false so that other
        // writer threads can continue to append message entry to the writeCache4Append.
        canSwap.set(false, canSwap.getStamp() + 1);
        this.writeCache4Flush = temp;
        flushLock.unlock();
    }

    public void clearFlushedCache() {
        writeCache4Flush.clear();
    }

}
