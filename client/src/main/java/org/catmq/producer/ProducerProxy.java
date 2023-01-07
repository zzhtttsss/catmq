package org.catmq.producer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.producer.balance.ILoadBalance;
import org.catmq.producer.balance.LeastUsedStrategy;
import org.catmq.producer.balance.RoundRobinStrategy;

import java.util.Optional;

@Slf4j
public class ProducerProxy {
    @Getter
    private final ILoadBalance loadBalance;

    /**
     * Get optimal broker address from zookeeper using the specified algorithm
     *
     * @param zkAddress zookeeper address
     * @return broker address path like /address/broker/127.0.0.1:5432
     */
    public Optional<String> selectBroker(String zkAddress) {
        return loadBalance.selectBroker(zkAddress);
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
