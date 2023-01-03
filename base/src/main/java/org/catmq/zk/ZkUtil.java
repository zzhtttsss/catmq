package org.catmq.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;

/**
 * @author BYL
 */
public class ZkUtil {
    public static CuratorFramework createClient(String zkAddress) {
        RetryPolicy policy = new RetryOneTime(1000);
        return CuratorFrameworkFactory.newClient(zkAddress, policy);
    }

}
