package org.catmq.zk;


import org.apache.zookeeper.WatchedEvent;

/**
 * @author BYL
 */
@FunctionalInterface
public interface INodeCreated {
    /**
     * This method is used to process the event of NodeCreated.
     *
     * @param watchedEvent WatchedEvent passed by zookeeper
     */
    void processNodeCreated(WatchedEvent watchedEvent);
}
