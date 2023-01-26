package org.catmq.zk;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.catmq.entity.BrokerInfo;

/**
 * @author BYL
 */
@Slf4j
public class DeadNodeListener implements CuratorCacheListener {

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
