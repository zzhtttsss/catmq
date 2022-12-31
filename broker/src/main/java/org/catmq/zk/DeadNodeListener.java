package org.catmq.zk;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.catmq.broker.BrokerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author BYL
 */
public class DeadNodeListener implements CuratorCacheListener {
    Logger log = LoggerFactory.getLogger(DeadNodeListener.class);

    private final ZkMutexLock lock;


    @Override
    public void event(Type type, ChildData oldData, ChildData data) {
        if (type == Type.NODE_DELETED) {
            try {
                lock.lock();
                log.warn("One node has been deleted, old data {}, new data {}", oldData, data);
                //TODO: handle the dead node
            } finally {
                lock.unlock();
            }
        } else {
            log.info("Event Type {}", type);
        }
    }

    public DeadNodeListener(BrokerInfo info) {
        this.lock = new ZkMutexLock("/lock", info.getZkAddress());
    }
}
