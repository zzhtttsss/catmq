package org.catmq.client.producer.balance;


import org.catmq.thread.ThreadFactoryWithIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Deprecated
public class RoundRobinStrategy implements LoadBalance {

    private List<String> brokerAddressList;
    private final AtomicInteger index;

    private ScheduledExecutorService timer;

    private RoundRobinStrategy() {
        this.brokerAddressList = new ArrayList<>();
        this.index = new AtomicInteger(0);
        this.timer = new ScheduledThreadPoolExecutor(1,
                new ThreadFactoryWithIndex("RoundRobinTimer_"));
        this.timer.scheduleAtFixedRate(() -> {
            if (index.get() >= brokerAddressList.size()) {
                index.set(0);
            }
        }, 0, 1, TimeUnit.MINUTES);
    }

    @Override
    public Optional<String> selectBroker(String zkAddress) {
        return Optional.empty();
    }

    public enum RoundRobinStrategyEnum {
        /**
         * singleton
         */
        INSTANCE;

        private final RoundRobinStrategy randRobbinStrategy;

        RoundRobinStrategyEnum() {
            randRobbinStrategy = new RoundRobinStrategy();
        }

        public RoundRobinStrategy getStrategy() {
            return randRobbinStrategy;
        }
    }
}
