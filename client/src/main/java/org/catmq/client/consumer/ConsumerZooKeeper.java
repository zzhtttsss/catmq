package org.catmq.client.consumer;

import org.catmq.zk.BaseZookeeper;

public class ConsumerZooKeeper extends BaseZookeeper {

    private final ConsumerConfig config;

    @Override
    protected void register2Zk() {

    }

    @Override
    protected void close() {
        super.client.close();
    }

    protected ConsumerZooKeeper(ConsumerConfig config) {
        super(null);
        this.config = config;
    }
}
