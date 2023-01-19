package org.catmq.broker.topic.nonpersistent;

import org.catmq.broker.topic.IDispatcher;

public interface INonPersistentDispatcher extends IDispatcher {
    /**
     * This dispatcher sends messages to all consumers connected.
     *
     * @param msg msg, this should be Entry in bk
     */
    void sendMessages(String msg);

}
