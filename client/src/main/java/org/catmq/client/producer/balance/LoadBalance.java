package org.catmq.client.producer.balance;

import org.apache.curator.framework.CuratorFramework;

import java.util.Optional;

@FunctionalInterface
public interface LoadBalance {
    /**
     * This method is used to select a broker address from the broker list.
     *
     * @param client zookeeper client
     * @return broker address path like /address/broker/127.0.0.1:5432
     */
    Optional<String[]> selectBroker(CuratorFramework client, int num);
}
