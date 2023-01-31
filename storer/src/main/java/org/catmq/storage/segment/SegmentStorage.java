package org.catmq.storage.segment;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.common.MessageEntry;
import org.catmq.common.MessageEntryBatch;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicStampedReference;
import java.util.concurrent.locks.ReentrantLock;

import static org.catmq.entity.StorerConfig.STORER_CONFIG;

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
    @Getter
    private final SegmentFileManager segmentFileManager;
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
     * Make sure that only one writer thread can {@link WriteCache} each time when {@link WriteCache}
     * is full. We use {@link AtomicStampedReference} to prevent the ABA problem.
     */
    public final AtomicStampedReference<Boolean> isSwapping = new AtomicStampedReference<>(false, 1);
    @Getter
    private final ConcurrentHashMap<Long, Segment> segments;


    /**
     * Append an {@link MessageEntry} to the writeCache4Append.
     *
     * @param messageEntry message entry need to be appended to writeCache4Append
     */
    @Deprecated
    public void appendEntry2WriteCache(MessageEntry messageEntry) {
        while (isSwapping.getReference()) {

        }
        int stamp = isSwapping.getStamp();
        boolean ok = writeCache4Append.appendEntry(messageEntry);
        if (!ok) {
            swapOrWait(stamp);
            // After swapping, try again.
            this.writeCache4Append.appendEntry(messageEntry);
        }
    }

    public void batchAppendEntry2WriteCache(MessageEntryBatch messageEntryBatch) {
        while (isSwapping.getReference()) {

        }
        int stamp = isSwapping.getStamp();
        boolean ok = writeCache4Append.batchAppendEntry(messageEntryBatch);
        if (!ok) {
            swapOrWait(stamp);
            // After swapping, try again.
            this.writeCache4Append.batchAppendEntry(messageEntryBatch);
        }

    }

    private void swapOrWait(int stamp) {
        // Only one writer thread can swap the writeCache.
        if (isSwapping.compareAndSet(false, true, stamp, stamp + 1)) {
            log.warn("Cas success, start swapping.");
            long current = System.nanoTime();
            while (System.nanoTime() - current < 10000 || !writeCache4Append.ready2Swap()) {

            }
            swapAndFlush();
        } else {
            log.warn("Cas fail, start waiting.");
            // Blocking if other writer thread is swapping the writeCache.
            while (isSwapping.getReference()) {

            }
        }
    }

    public Optional<MessageEntry> getEntryFromWriteCacheById(long segmentId, long entryId) {
        MessageEntry entry;
        while (isSwapping.getReference()) {
        }
        WriteCache localWriteCache4Append = writeCache4Append;
        WriteCache localWriteCache4Flush = writeCache4Flush;
        entry = localWriteCache4Append.getEntry(segmentId, entryId);
        if (entry != null) {
            return Optional.of(entry);
        }
        entry = localWriteCache4Flush.getEntry(segmentId, entryId);
        if (entry != null) {
            return Optional.of(entry);
        }
        return Optional.empty();
    }

    public Optional<MessageEntry> getEntryFromReadCacheById(long segmentId, long entryId) {
        return Optional.ofNullable(readCache.getEntry(segmentId, entryId));
    }

    public Optional<MessageEntry> getEntryFromFileById(long segmentId, long entryId) {
        try {
            long offset = entryPositionIndex.getPosition(segmentId, entryId);
            MessageEntryBatch batch = segmentFileManager.getSegmentBatchByOffset(offset);
            if (batch.isEmpty()) {
                log.info("Batch is empty.");
                return Optional.empty();
            }
            readCache.putBatch(batch);
            MessageEntry entry = batch.get(0);
            return Optional.ofNullable(entry);
        } catch (Exception e) {
            log.error("Get entry from file err, ", e);
            return Optional.empty();
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
        try {
            WriteCache temp = this.writeCache4Append;
            this.writeCache4Append = this.writeCache4Flush;
            this.writeCache4Flush = temp;
            // Ensure that the swapping finishes.
            isSwapping.set(false, isSwapping.getStamp() + 1);
        } finally {
            flushLock.unlock();
        }

    }

    public void clearFlushedCache() {
        writeCache4Flush.clear();
    }


    private SegmentStorage() {
        this.path = STORER_CONFIG.getSegmentStoragePath();
        this.writeCache4Append = new WriteCache(MAX_CACHE_SIZE);
        this.writeCache4Flush = new WriteCache(MAX_CACHE_SIZE);
        this.readCache = new ReadCache(MAX_CACHE_SIZE);
        this.segmentFileManager = SegmentFileManager.SegmentFileServiceEnum.INSTANCE.getInstance();
        this.segments = new ConcurrentHashMap<>();
        this.flushWriteCacheService = new FlushWriteCacheService(this, segmentFileManager);
        this.entryPositionIndex = new EntryPositionIndex(KeyValueStorageRocksDB.factory,
                STORER_CONFIG.getSegmentIndexStoragePath());
        this.flushWriteCacheService.start();
    }

    public enum SegmentStorageEnum {
        INSTANCE;
        private final SegmentStorage segmentStorage;

        SegmentStorageEnum() {
            segmentStorage = new SegmentStorage();
        }

        public SegmentStorage getInstance() {
            return segmentStorage;
        }
    }
}
