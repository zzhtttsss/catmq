package org.catmq.broker.zk;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

@Slf4j
public class ZkIdGeneratorTest {
    private static TestingServer server;

    @BeforeClass
    public static void beforeClass() throws Exception {
        server = new TestingServer();
        server.start();
    }

    @AfterClass
    public static void afterClass() throws IOException {
        server.close();
    }

    @Test
    public void testNextId() throws InterruptedException {
//        BrokerConfig config = BrokerConfig.BrokerConfigEnum.INSTANCE.getInstance();
//        config.setZkAddress(server.getConnectString());
//        ZkIdGenerator idGenerator = ZkIdGenerator.ZkIdGeneratorEnum.INSTANCE.getInstance();
//        ExecutorService executors = Executors.newFixedThreadPool(10);
//        Set<Long> sets = new ConcurrentHashSet<>();
//        for (int i = 0; i < 10; i++) {
//            CompletableFuture.runAsync(() -> {
//                for (int j = 0; j < 100; j++) {
//                    sets.add(idGenerator.nextId());
//                }
//            }, executors);
//        }
//        executors.shutdown();
//        executors.awaitTermination(100, TimeUnit.SECONDS);
//        Assert.assertEquals(1000, sets.size());
    }
}
