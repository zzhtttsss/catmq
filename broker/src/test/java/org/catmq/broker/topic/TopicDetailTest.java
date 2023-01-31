package org.catmq.broker.topic;

import org.catmq.entity.TopicType;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class TopicDetailTest {
    @Test
    public void testTopicType() {
        String domain = "persistent";
        Assert.assertEquals(TopicType.PERSISTENT, TopicType.fromString(domain));
        domain = "non-persistent";
        Assert.assertEquals(TopicType.NON_PERSISTENT, TopicType.fromString(domain));
        final String finalDomain = "nonpersistent";
        Assert.assertThrows(IllegalArgumentException.class, () -> TopicType.fromString(finalDomain));
    }

    @Test
    public void testlsf() throws InterruptedException {
        AtomicInteger a = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(() -> {
                for (int j = 0; j < 100; j++) {
                    a.incrementAndGet();
                }
                latch.countDown();
            });
            thread.start();
        }
        latch.await();
        System.out.println(a.get());
    }
}
