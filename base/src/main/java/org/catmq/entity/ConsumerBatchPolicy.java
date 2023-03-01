package org.catmq.entity;

import com.alibaba.fastjson2.JSON;

public class ConsumerBatchPolicy implements JsonSerializable {
    private final int consumerBatchNumber;
    private final long consumerTimeout;

    public int getBatchNumber() {
        return consumerBatchNumber;
    }

    public long getTimeoutInMs() {
        return consumerTimeout;
    }

    @Override
    public byte[] toBytes() {
        return JSON.toJSONBytes(this);
    }

    public static class ConsumerBatchPolicyBuilder {
        private int batchNumber = 10;
        private long timeout = 1000;

        public static ConsumerBatchPolicyBuilder builder() {
            return new ConsumerBatchPolicyBuilder();
        }

        public ConsumerBatchPolicyBuilder setBatchNumber(int num) {
            this.batchNumber = num;
            return this;
        }

        public ConsumerBatchPolicyBuilder setTimeoutInMs(long ms) {
            this.timeout = ms;
            return this;
        }

        public ConsumerBatchPolicy build() {
            return new ConsumerBatchPolicy(this.batchNumber, this.timeout);
        }
    }

    private ConsumerBatchPolicy(int num, long time) {
        this.consumerBatchNumber = num;
        this.consumerTimeout = time;
    }
}
