package org.catmq.client.producer.balance;

import java.util.Optional;

@FunctionalInterface
public interface ILoadBalance {
    /**
     * This method is used to select a broker address from the broker list.
     *
     * @param zkAddress zookeeper address
     * @return broker address path like /address/broker/127.0.0.1:5432
     */
    Optional<String> selectBroker(String zkAddress);
}
