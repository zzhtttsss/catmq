package org.catmq;

import static org.catmq.util.ConfigUtil.PROCESSOR_NUMBER;

public class BrokerConfig {

    private BrokerConfig() {
    }


    private int grpcProducerThreadQueueCapacity = 10000;

    private int grpcProducerThreadPoolNums = PROCESSOR_NUMBER;


    public int getGrpcProducerThreadQueueCapacity() {
        return grpcProducerThreadQueueCapacity;
    }

    public void setGrpcProducerThreadQueueCapacity(int grpcProducerThreadQueueCapacity) {
        this.grpcProducerThreadQueueCapacity = grpcProducerThreadQueueCapacity;
    }

    public int getGrpcProducerThreadPoolNums() {
        return grpcProducerThreadPoolNums;
    }

    public void setGrpcProducerThreadPoolNums(int grpcProducerThreadPoolNums) {
        this.grpcProducerThreadPoolNums = grpcProducerThreadPoolNums;
    }

    public enum BrokerConfigEnum {
        INSTANCE;
        private final BrokerConfig brokerConfig;

        BrokerConfigEnum() {
            brokerConfig = new BrokerConfig();
        }

        public BrokerConfig getInstance() {
            return brokerConfig;
        }
    }
}
