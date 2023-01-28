package org.catmq.storage.segment;

import lombok.extern.slf4j.Slf4j;
import org.catmq.thread.ServiceThread;

import java.util.concurrent.locks.ReentrantLock;

import static org.catmq.entity.StorerConfig.STORER_CONFIG;

@Slf4j
public class CleanReadCacheService extends ServiceThread {

    private final ReadCache cache;
    private final ReentrantLock lock;

    private final long maxCacheSize;


    public void cleanCache(long batchSize) {
        if (batchSize < 0) {
            // Big clean-up occurs
            // Clean up an expired or deletable key.
            final long threshold = (long) (maxCacheSize * STORER_CONFIG.getReadCacheRemainingThreshold());
            cleanup0(threshold);
        } else {
            // double check
            if (cache.getCacheSize().get() > maxCacheSize) {
                cleanup0(maxCacheSize);
            }

        }
    }

    private void cleanup0(long threshold) {
        lock.lock();
        try {
            // Just clean up the oldest segment with its entries.
            for (var key : cache.getCache().ascendingKeySet()) {
                cache.getCacheSize().addAndGet(-key.getBatchSegmentSize());
                cache.getCache().remove(key);
                if (cache.getCacheSize().get() <= threshold) {
                    return;
                }
            }
            if (cache.getCacheSize().get() > threshold) {
                log.error("ReadCache is still full after cleaning up.");
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String getServiceName() {
        return this.getClass().getName();
    }

    @Override
    public void run() {
        long lastFlushTime = System.currentTimeMillis();
        while (!this.isStopped()) {
            // Big Clean-up occurs when time is up or there are too many batch in queue.
            if (System.currentTimeMillis() - lastFlushTime > STORER_CONFIG.getReadCacheCleanUpInterval()) {
                this.cleanCache(-1);
                log.info("{} has cleaned up.", this.getServiceName());
                lastFlushTime = System.currentTimeMillis();
            }
        }
    }

    public CleanReadCacheService(ReadCache readCache) {
        this.cache = readCache;
        this.maxCacheSize = readCache.getMaxCacheSize();
        this.lock = new ReentrantLock();
    }
}
