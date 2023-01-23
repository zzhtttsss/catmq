package org.catmq.storage.segment;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.catmq.common.MessageEntry;
import org.catmq.common.MessageEntryBatch;
import org.catmq.thread.ServiceThread;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.catmq.storer.StorerConfig.STORER_CONFIG;

@Slf4j
public class ReadCache extends ServiceThread {

    private final long maxCacheSize;

    @Getter
    private AtomicLong cacheSize;

    private final ReentrantReadWriteLock lock;
    @Getter
    private final LinkedHashMap<ReadCacheKey, ReadCacheValue> cache;

    /**
     * Put a {@link MessageEntry} into cache whose key are segmentId and entryId.
     *
     * @param messageEntry msg to be put
     */
    public void putEntry(MessageEntry messageEntry) {
        if (this.cacheSize.addAndGet(messageEntry.getTotalSize()) > maxCacheSize) {
            cleanCache();
        }
        ReadCacheKey key = new ReadCacheKey(messageEntry.getSegmentId(), messageEntry.getEntryId());
        ReadCacheValue value = new ReadCacheValue(messageEntry);
        lock.writeLock().lock();
        try {
            cache.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void putBatch(MessageEntryBatch batch) {
        long totalSize = batch.getTotalSize();
        if (this.cacheSize.addAndGet(totalSize) > maxCacheSize) {
            cleanCache();
        }
        lock.writeLock().lock();
        try {
            for (MessageEntry messageEntry : batch.getBatch()) {
                ReadCacheKey key = new ReadCacheKey(messageEntry.getSegmentId(), messageEntry.getEntryId());
                ReadCacheValue value = new ReadCacheValue(messageEntry);
                cache.put(key, value);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public MessageEntry getEntry(long segmentId, long entryId) {
        ReadCacheKey key = new ReadCacheKey(segmentId, entryId);
        lock.readLock().lock();
        try {
            ReadCacheValue value = cache.get(key);
            if (value == null) {
                return null;
            }
            value.setDeletable();
            return value.getMessage();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public String getServiceName() {
        return this.getClass().getCanonicalName();
    }

    @Override
    public void run() {
        log.info("{} start to clean up expired keys.", this.getServiceName());
        while (!this.isStopped()) {
            try {
                Thread.sleep(STORER_CONFIG.getReadCacheCleanUpInterval());
            } catch (InterruptedException e) {
                log.error("ReadCache thread is interrupted.");
            }
            lock.writeLock().lock();
            try {
                for (Map.Entry<ReadCacheKey, ReadCacheValue> entry : cache.entrySet()) {
                    if (entry.getValue().isExpired() || entry.getValue().deletable()) {
                        cache.remove(entry.getKey());
                        cacheSize.addAndGet(-entry.getValue().getMessageSize());
                    }
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
        log.info("{} has cleaned up expired keys.", this.getServiceName());
    }

    private void cleanCache() {
        lock.writeLock().lock();
        try {
            // Clean up an expired or deletable key.
            for (Map.Entry<ReadCacheKey, ReadCacheValue> entry : cache.entrySet()) {
                if (entry.getValue().deletable()) {
                    cache.remove(entry.getKey());
                    cacheSize.addAndGet(-entry.getValue().getMessageSize());
                    if (cacheSize.get() <= maxCacheSize) {
                        return;
                    }
                }
            }
            // If the cache is still full, delete the remaining.
            var iterator = cache.entrySet().iterator();
            while (cacheSize.get() > maxCacheSize && iterator.hasNext()) {
                var entry = iterator.next();
                cache.remove(entry.getKey());
                cacheSize.addAndGet(-entry.getValue().getMessageSize());
            }
            if (cacheSize.get() > maxCacheSize) {
                log.error("ReadCache is still full after cleaning up.");
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public ReadCache(long cacheSize) {
        this.maxCacheSize = cacheSize;
        this.cacheSize = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.cache = new LinkedHashMap<>(8, 0.75f, true);
    }
}


record ReadCacheKey(long segmentId, long entryId) {

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        ReadCacheKey readCacheKey = (ReadCacheKey) obj;
        return readCacheKey.segmentId == this.segmentId && readCacheKey.entryId == this.entryId;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(segmentId) + Long.hashCode(entryId);
    }
}

class ReadCacheValue implements Comparable<ReadCacheValue> {
    private final MessageEntry messageEntry;
    private final long expiredTime;

    private boolean deleted;

    public boolean isExpired() {
        return System.currentTimeMillis() > expiredTime;
    }

    public MessageEntry getMessage() {
        return messageEntry;
    }

    public void setDeletable() {
        this.deleted = true;
    }

    public boolean deletable() {
        return deleted;
    }

    public long getMessageSize() {
        return messageEntry.getTotalSize();
    }

    @Override
    public int compareTo(@NonNull ReadCacheValue o) {
        if (this.deletable() && !o.deletable()) {
            return 1;
        } else if (!this.deletable() && o.deletable()) {
            return -1;
        } else if (this.expiredTime == o.expiredTime) {
            return 0;
        } else {
            return this.expiredTime > o.expiredTime ? 1 : -1;
        }
    }

    ReadCacheValue(MessageEntry messageEntry) {
        this.messageEntry = messageEntry;
        long createdTime = System.currentTimeMillis();
        this.expiredTime = createdTime + STORER_CONFIG.getReadCacheExpireTime();
        this.deleted = false;
    }
}
