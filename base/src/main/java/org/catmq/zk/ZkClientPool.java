package org.catmq.zk;

import lombok.Builder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

import java.util.LinkedList;

@Builder
@Deprecated // deadlock not fixed
public class ZkClientPool {
    private final LinkedList<CuratorFramework> pool = new LinkedList<>();
    private final int maxPoolSize;
    private final String connectString;
    private final int sessionTimeoutMs;
    private final int connectionTimeoutMs;
    private final int retryCount;

    public synchronized CuratorFramework acquire() throws Exception {
        if (pool.isEmpty()) {
            if (0 < maxPoolSize) {
                return create();
            } else {
                this.wait();
                return acquire();
            }
        } else {
            return pool.pop();
        }
    }

    public synchronized void release(CuratorFramework client) {
        if (pool.size() < maxPoolSize) {
            pool.push(client);
            this.notify();
        } else {
            client.close();
        }
    }

    private CuratorFramework create() {
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString,
                sessionTimeoutMs, connectionTimeoutMs, new RetryNTimes(retryCount, 1000));
        client.start();
        return client;
    }
}
