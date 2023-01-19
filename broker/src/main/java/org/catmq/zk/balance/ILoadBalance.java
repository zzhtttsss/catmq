package org.catmq.zk.balance;

import org.catmq.broker.BrokerInfo;
import org.catmq.command.BooleanError;

/**
 * @author BYL
 */

public interface ILoadBalance {
    /**
     * This method is used to register some infos to support load balance.
     *
     * @param info broker info.
     */
    BooleanError registerConnection(BrokerInfo info);

    /**
     * This method is used to get the optimal client.
     *
     * @return the address of client like /address/broker/IP:PORT
     */
    String getOptimalConnection();
}
