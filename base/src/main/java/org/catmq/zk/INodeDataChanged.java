package org.catmq.zk;

import org.apache.zookeeper.WatchedEvent;

/**
 * @author BYL
 */
@FunctionalInterface
public interface INodeDataChanged {
    /**
     * This method is used to process the event of NodeDataChanged.
     *
     * @param watchedEvent WatchedEvent passed by zookeeper
     */
    void processNodeDataChanged(WatchedEvent watchedEvent);
}
