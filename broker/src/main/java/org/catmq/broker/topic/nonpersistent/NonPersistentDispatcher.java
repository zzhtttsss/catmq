package org.catmq.broker.topic.nonpersistent;

import org.catmq.broker.topic.Dispatcher;
import org.catmq.protocol.definition.OriginMessage;

import java.util.List;

public interface NonPersistentDispatcher extends Dispatcher {
    /**
     * This dispatcher sends messages to all consumers connected.
     *
     * @param msg msg, this should be Entry in bk
     */
    void sendMessages(List<OriginMessage> msg);

}
