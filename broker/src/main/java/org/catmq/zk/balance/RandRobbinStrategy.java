package org.catmq.zk.balance;

import org.catmq.broker.BrokerServer;
import org.catmq.command.BooleanError;

/**
 * @author BYL
 */
public class RandRobbinStrategy implements ILoadBalance {

    @Override
    public BooleanError registerConnection(BrokerServer server) {
        return null;
    }

    @Override
    public String getOptimalConnection() {
        return null;
    }
}
