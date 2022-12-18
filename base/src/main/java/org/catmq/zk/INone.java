package org.catmq.zk;

import org.apache.zookeeper.WatchedEvent;

/**
 * @author BYL
 */
@FunctionalInterface
public interface INone {
    /**
     * This method is used to process the event of None.
     *
     * @param watchedEvent WatchedEvent passed by zookeeper
     */
    void processNone(WatchedEvent watchedEvent);
}
