package org.catmq.zk;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

@Slf4j
public abstract class BaseZookeeper {
    public final CuratorFramework client;

    /**
     * This method is used to register the broker to zookeeper.
     */
    protected abstract void register2Zk();

    /**
     * This method is used to close resources.
     */
    protected abstract void close();

    protected BaseZookeeper(String zkAddress) {
        RetryPolicy policy = new ExponentialBackoffRetry(1000, 3);
        this.client = CuratorFrameworkFactory.newClient(zkAddress, policy);
        client.start();
    }
}
