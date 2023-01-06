package org.catmq.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;

import java.util.concurrent.TimeUnit;

/**
 * @author BYL
 */
public class ZkUtil {
    public static CuratorFramework createClient(String zkAddress) throws InterruptedException {
        RetryPolicy policy = new RetryOneTime(1000);
        CuratorFramework cf = CuratorFrameworkFactory.newClient(zkAddress, policy);
        cf.start();
        return cf;
    }

    public static CuratorFramework createClient(String zkAddress, int timeout, TimeUnit unit) throws InterruptedException {
        RetryPolicy policy = new RetryOneTime(1000);
        CuratorFramework cf = CuratorFrameworkFactory.newClient(zkAddress, policy);
        if (cf.blockUntilConnected(timeout, unit)) {
            return cf;
        }
        throw new RuntimeException("Can not connect to zk server.");
    }

}
