package org.catmq.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import static org.apache.curator.framework.imps.CuratorFrameworkState.STARTED;

public class ZkUtilTest {
    @Test
    public void testCreateClient() throws Exception {
        RetryPolicy policy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework cf = CuratorFrameworkFactory.newClient("localhost:2181", policy);
        while (cf.getState() != STARTED) {
            Thread.sleep(100);
        }
        byte[] bytes = cf.getData().forPath("/broker");
        System.out.println(new String(bytes));
        cf.close();
    }

    @Test
    public void test() throws Exception {
        ZooKeeper zk = new ZooKeeper("localhost:2181", 1000, null);
        while (zk.getState() != ZooKeeper.States.CONNECTED) {
            Thread.sleep(100);
        }
        zk.getChildren("/zookeeper", null, null).forEach(System.out::println);
        zk.close();
    }
}
