package org.catmq.broker.manager;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TopicManagerTest {
    @Test
    public void testAddAndGetNumber() throws InterruptedException {
        TopicManager topicManager = TopicManager.TopicManagerEnum.INSTANCE.getInstance();
        ExecutorService executors = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 100; i++) {
            int finalI = i;
            executors.execute(() -> {
                for (int j = 0; j < 1000; j++) {
                    topicManager.addSegmentNumber(finalI % 10, 2);
                }

            });
        }
        executors.shutdown();
        executors.awaitTermination(1, TimeUnit.MINUTES);
        Assert.assertEquals(20000, topicManager.getSegmentTotalNumber(1));
    }
}
