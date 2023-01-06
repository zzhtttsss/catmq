package org.catmq.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

public class ZkUtilTest {
    @Test
    public void testCreateClient() throws Exception {
        try (CuratorFramework cf = ZkUtil.createClient("127.0.0.1:2181")) {
            byte[] bytes = cf.getData().forPath("/zookeeper");
            System.out.println(new String(bytes));
        }
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
