package org.catmq.broker.topic.nonpersistent;

import org.catmq.broker.topic.Dispatcher;

public interface NonPersistentDispatcher extends Dispatcher {
    /**
     * This dispatcher sends messages to all consumers connected.
     *
     * @param msg msg, this should be Entry in bk
     */
    void sendMessages(byte[] msg);

}
