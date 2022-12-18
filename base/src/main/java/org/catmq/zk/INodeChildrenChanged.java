package org.catmq.zk;

import org.apache.zookeeper.WatchedEvent;

/**
 * @author BYL
 */
@FunctionalInterface
public interface INodeChildrenChanged {
    /**
     * This method is used to process the event of NodeChildrenChanged.
     *
     * @param watchedEvent WatchedEvent passed by zookeeper
     */
    void processNodeChildrenChanged(WatchedEvent watchedEvent);
}
