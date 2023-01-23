package org.catmq.client.producer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.catmq.client.producer.balance.LoadBalance;
import org.catmq.client.producer.balance.LeastUsedStrategy;
import org.catmq.client.producer.balance.RoundRobinStrategy;

import java.util.Optional;

@Slf4j
public class ProducerProxy {
    @Getter
    private final LoadBalance loadBalance;

    /**
     * Get optimal broker address from zookeeper using the specified algorithm
     *
     * @param client zookeeper client
     * @return broker address path like /address/broker/127.0.0.1:5432
     */
    public Optional<String[]> selectBrokers(CuratorFramework client, int num) {
        return loadBalance.selectBroker(client, num);
    }

    public ProducerProxy(LoadBalanceEnum loadBalanceEnum) {
        switch (loadBalanceEnum) {
            case LEAST_USED -> this.loadBalance = LeastUsedStrategy.LeastUsedStrategyEnum.INSTANCE.getStrategy();
            case ROUND_ROBIN -> this.loadBalance = RoundRobinStrategy.RoundRobinStrategyEnum.INSTANCE.getStrategy();
            default -> {
                log.warn("Load balance strategy not found, use default strategy: round robin");
                this.loadBalance = RoundRobinStrategy.RoundRobinStrategyEnum.INSTANCE.getStrategy();
            }
        }
    }

    public enum LoadBalanceEnum {
        /**
         * least used
         */
        LEAST_USED,
        /**
         * round robin
         */
        ROUND_ROBIN;
    }
}
