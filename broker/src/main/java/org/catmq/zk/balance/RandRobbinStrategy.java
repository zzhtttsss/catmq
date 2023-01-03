package org.catmq.zk.balance;

import org.catmq.broker.BrokerInfo;
import org.catmq.command.BooleanError;

/**
 * @author BYL
 */
public class RandRobbinStrategy implements ILoadBalance {
    
    @Override
    public BooleanError registerConnection(BrokerInfo info) {
        return null;
    }

    @Override
    public String getOptimalConnection() {
        return null;
    }
}
