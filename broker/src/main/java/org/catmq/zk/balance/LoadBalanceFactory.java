package org.catmq.zk.balance;

import lombok.Getter;

/**
 * @author BYL
 */
public enum LoadBalanceFactory {
    /**
     *
     */
    LEAST_USED(new LeastUsedStrategy());

    @Getter
    private final ILoadBalance strategy;

    LoadBalanceFactory(ILoadBalance strategy) {
        this.strategy = strategy;
    }
}
