package org.catmq.zk;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author BYL
 */
@Slf4j
public abstract class BaseZookeeper {
    public final CuratorFramework client;

    /**
     * This method is used to register the broker to zookeeper.
     */
    protected abstract void register2Zk();

    /**
     * This method is used to get the client who has the minimum number of connections on ZooKeeper.
     *
     * @return the address of client like /address/broker/IP:PORT
     */
    protected abstract String getOptimalConnection();

    /**
     * This method is used to close resources.
     */
    protected abstract void close();

    /**
     * This method is used to increase the number of connections on specified client.
     *
     * @param path the path target client.
     */
    protected void increaseTheNumberOfRequestedSessions(String path) {
        try {
            byte[] bytes = client.getData().forPath(path);
            int count = Integer.parseInt(new String(bytes));
            count++;
            client.setData().forPath(path, String.valueOf(count).getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This method is used to decrease the number of connections on specified client.
     *
     * @param path the path of target client.
     */
    protected void decreaseTheNumberOfRequestedSessions(String path) {
        try {
            byte[] bytes = client.getData().forPath(path);
            int count = Integer.parseInt(new String(bytes));
            if (count <= 0) {
                log.error("The number of connections on {} is less than 0.", path);
                return;
            }
            count--;
            client.setData().forPath(path, String.valueOf(count).getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected BaseZookeeper(String zkAddress) {
        RetryPolicy policy = new ExponentialBackoffRetry(1000, 3);
        this.client = CuratorFrameworkFactory.newClient(zkAddress, policy);
    }
}
