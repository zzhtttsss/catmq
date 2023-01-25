package org.catmq.storage.segment;

import org.catmq.common.MessageEntry;
import org.catmq.common.MessageEntryBatch;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static org.catmq.storer.StorerConfig.STORER_CONFIG;


public class ReadCacheTest {

    static ReadCache cache;
    static String defaultValue;


    @BeforeClass
    public static void beforeClass() {
        cache = new ReadCache(STORER_CONFIG.getSegmentMaxFileSize());
        defaultValue = "hello world 2023";
    }

    @Test
    public void testPutWithoutThread() {
        SegmentBatchKey key1 = new SegmentBatchKey(1);
        SegmentBatchKey key2 = new SegmentBatchKey(1);
        Assert.assertEquals(key1, key2);
        SegmentBatchKey key3 = new SegmentBatchKey(2);
        Assert.assertNotEquals(key1, key3);
        cache.putEntry(new MessageEntry(1, 1, defaultValue.getBytes()));
        cache.putEntry(new MessageEntry(2, 1, defaultValue.getBytes()));
        MessageEntry entry = cache.getEntry(1, 1);
        Assert.assertEquals(entry.getSegmentId(), 1);
    }

    @Test
    public void testAccess() {
        cache.putEntry(new MessageEntry(1, 1, defaultValue.getBytes()));
        cache.putEntry(new MessageEntry(2, 1, defaultValue.getBytes()));
        cache.putEntry(new MessageEntry(3, 1, defaultValue.getBytes()));
        cache.putEntry(new MessageEntry(4, 1, defaultValue.getBytes()));
        cache.getEntry(1, 1);
        SegmentBatchKey[] keys = cache.getCache().ascendingKeySet().toArray(SegmentBatchKey[]::new);
        int n = keys.length;
        Assert.assertEquals(1, keys[n - 1].getSegmentId());
    }

    @Test
    public void testBatchWithThread() throws InterruptedException {
        class PutThread extends Thread {
            final int segmentId;
            final CountDownLatch latch;

            PutThread(int segmentId, CountDownLatch latch) {
                this.segmentId = segmentId;
                this.latch = latch;
            }

            @Override
            public void run() {
                MessageEntryBatch batch = new MessageEntryBatch();
                for (int i = 0; i < 10; i++) {
                    MessageEntry messageEntry = new MessageEntry(segmentId, i,
                            String.format("%d@%d", segmentId, i).getBytes());
                    batch.put(messageEntry);
                }
                cache.putBatch(batch);
                latch.countDown();
            }
        }
        CountDownLatch latch = new CountDownLatch(3);
        for (int i = 0; i < 3; i++) {
            new PutThread(i, latch).start();
        }
        latch.await();
        MessageEntry entry = cache.getEntry(0, 1);
        Assert.assertEquals("0@1", new String(entry.getMessage()));
        SegmentBatchKey[] keys = cache.getCache().ascendingKeySet().toArray(SegmentBatchKey[]::new);
        int n = keys.length;
        Assert.assertEquals(0, keys[n - 1].getSegmentId());
    }

    @Test
    public void testCleanup() throws InterruptedException {
        cache.start();
        CountDownLatch latch = new CountDownLatch(5);
        for (int i = 0; i < 5; i++) {
            int finalI = i;
            new Thread(() -> {
                for (int j = 0; j < 100; j++) {
                    MessageEntry messageEntry = new MessageEntry(finalI, j, defaultValue.getBytes());
                    cache.putEntry(messageEntry);
                }
                latch.countDown();
            }).start();
        }
        latch.await();
        Thread.sleep(1000);
    }
}
