package org.catmq.client.common;

import org.catmq.entity.BrokerInfo;

public interface ClientProxy {
    /**
     * Increase the number of requests sent to the specified broker
     *
     * @param brokerAddress the path target like /address/broker/ip:port.
     */
    void increaseRequestedCount(String brokerAddress);

    /**
     * Decrease the number of requests sent to the specified broker
     *
     * @param brokerAddress the path target like /address/broker/ip:port.
     */
    void decreaseRequestedCount(String brokerAddress);

    /**
     * Get broker information where this producer connected from zk.
     * <strong>One producer connects with many brokers which has not been considered yet</strong>
     *
     * @param brokerAddress the path target like /address/broker/ip:port.
     * @return broker information
     */
    default BrokerInfo getBrokerInfo(String brokerAddress) {
        throw new UnsupportedOperationException();
    }
}
