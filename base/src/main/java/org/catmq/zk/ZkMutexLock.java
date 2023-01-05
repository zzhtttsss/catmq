package org.catmq.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

/**
 * @author BYL
 */
public class ZkMutexLock {

    private final InterProcessMutex lock;

    public void lock() {
        try {
            lock.acquire();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void unlock() {
        try {
            lock.release();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public ZkMutexLock(String lockPath, String zkAddress) {
        try (CuratorFramework client = ZkUtil.createClient(zkAddress)) {
            this.lock = new InterProcessMutex(client, lockPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
