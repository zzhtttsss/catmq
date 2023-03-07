package org.catmq.broker.manager;

import org.catmq.broker.common.NumberedMessageBatch;
import org.catmq.protocol.definition.NumberedMessage;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ReadCacheManagerTest {

    @Test
    public void testCreateTopic() throws InterruptedException {
        ReadCacheManager readCacheManager = ReadCacheManager.ReadCacheManagerEnum.INSTANCE.getInstance();
        NumberedMessage message = NumberedMessage.newBuilder().setSegmentId(1).setEntryId(1).build();
        ExecutorService service = Executors.newFixedThreadPool(100);
        for (int i = 0; i < 1000; i++) {
            service.submit(() -> {
                for (int j = 0; j < 100; j++) {
                    readCacheManager.putEntry(UUID.randomUUID().toString(), message);
                }
            });
        }
        service.shutdown();
        service.awaitTermination(1, TimeUnit.MINUTES);
        Assert.assertEquals(100000, readCacheManager.getCache().size());
    }

    @Test
    public void testGetEntryInSingleTopic() {
        ReadCacheManager readCacheManager = ReadCacheManager.ReadCacheManagerEnum.INSTANCE.getInstance();
        String topicName = "persistent:normal:$public:testtopic";
        for (int i = 0; i < 100; i++) {
            readCacheManager.putEntry(topicName, NumberedMessage
                    .newBuilder()
                    .setSegmentId(1)
                    .setEntryId(i)
                    .build());
        }
        var opt = readCacheManager.getEntryBatch(topicName, 1, 1);
        Assert.assertTrue(opt.isPresent());
        NumberedMessageBatch batch = opt.get();
        Assert.assertEquals(1, batch.getSegmentId());
        Assert.assertEquals(1, batch.getBatch().size());
        Assert.assertEquals(1, batch.getBatch().get(0).getEntryId());
    }

    @Test
    public void testGetBatchInSingleTopic() {
        ReadCacheManager readCacheManager = ReadCacheManager.ReadCacheManagerEnum.INSTANCE.getInstance();
        String topicName = "persistent:normal:$public:testtopic";
        NumberedMessageBatch batch1 = new NumberedMessageBatch(1);
        for (int i = 0; i < 10; i++) {
            batch1.addMessage(NumberedMessage
                    .newBuilder()
                    .setSegmentId(1)
                    .setEntryId(i)
                    .build());
        }
        NumberedMessageBatch batch2 = new NumberedMessageBatch(1);
        for (int i = 10; i < 30; i++) {
            batch2.addMessage(NumberedMessage
                    .newBuilder()
                    .setSegmentId(1)
                    .setEntryId(i)
                    .build());
        }
        readCacheManager.putBatch(topicName, batch1);
        readCacheManager.putBatch(topicName, batch2);
        var opt = readCacheManager.getEntryBatch(topicName, 1, 5);
        Assert.assertTrue(opt.isPresent());
        NumberedMessageBatch batch = opt.get();
        Assert.assertEquals(1, batch.getSegmentId());
        Assert.assertEquals(10, batch.getBatch().size());
        var opt2 = readCacheManager.getEntryBatch(topicName, 1, 15);
        Assert.assertTrue(opt2.isPresent());
        NumberedMessageBatch batchb = opt2.get();
        Assert.assertEquals(1, batchb.getSegmentId());
        Assert.assertEquals(20, batchb.getBatch().size());
    }

    @Test
    public void testGetBatchInMultiTopics() {
        ReadCacheManager readCacheManager = ReadCacheManager.ReadCacheManagerEnum.INSTANCE.getInstance();
        String topicName1 = "persistent:normal:$public:testtopic1";
        String topicName2 = "persistent:normal:$public:testtopic2";
        NumberedMessageBatch batch1 = new NumberedMessageBatch(1);
        for (int i = 0; i < 10; i++) {
            batch1.addMessage(NumberedMessage
                    .newBuilder()
                    .setSegmentId(1)
                    .setEntryId(i)
                    .build());
        }
        readCacheManager.putBatch(topicName1, batch1);
        readCacheManager.putBatch(topicName2, batch1);
        var opt = readCacheManager.getEntryBatch(topicName1, 1, 5);
        Assert.assertTrue(opt.isPresent());
        NumberedMessageBatch batch = opt.get();
        Assert.assertEquals(1, batch.getSegmentId());
    }

    @Test
    public void testSegmentBatchKeyEquality() {
        SegmentBatchKey key1 = new SegmentBatchKey(1, 1, 1);
        SegmentBatchKey key2 = new SegmentBatchKey(1, 2, 2);
        System.out.println(key1.equals(key2));
        Assert.assertNotEquals(key1, key2);
    }
}
