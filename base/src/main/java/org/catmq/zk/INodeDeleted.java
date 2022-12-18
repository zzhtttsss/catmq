package org.catmq.zk;

import org.apache.zookeeper.WatchedEvent;

/**
 * @author BYL
 */
@FunctionalInterface
public interface INodeDeleted {
    /**
     * This method is used to process the event of NodeDeleted.
     *
     * @param watchedEvent WatchedEvent passed by zookeeper
     */
    void processNodeDeleted(WatchedEvent watchedEvent);
}
