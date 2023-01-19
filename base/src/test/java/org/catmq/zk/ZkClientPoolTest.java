package org.catmq.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public class ZkClientPoolTest {
    private static TestingServer server;

    @BeforeClass
    public static void beforeClass() throws Exception {
        server = new TestingServer();
        server.start();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        server.close();
    }

    @Test
    public void testZkClientPool() throws ExecutionException, InterruptedException {
        ZkClientPool zkClientPool = ZkClientPool
                .builder()
                .connectString(server.getConnectString())
                .maxPoolSize(1)
                .build();
        Supplier<CuratorFramework> supplier = () -> {
            try {
                return zkClientPool.acquire();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        };
        CompletableFuture<CuratorFramework> future1 = CompletableFuture.supplyAsync(supplier);
        future1.thenAccept(client -> {
            System.out.println("----------------");
            System.out.println(client.getState().name());
        });


    }
}


