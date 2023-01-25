package org.catmq.broker;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.catmq.common.GrpcConnectCache;
import org.catmq.zk.ZkUtil;

import static org.catmq.broker.BrokerConfig.BROKER_CONFIG;

/**
 * Broker with every service
 */
@Slf4j
@Getter
public class Broker {

    private BrokerInfo brokerInfo;

    public static final GrpcConnectCache GRPC_CONNECT_CACHE = new GrpcConnectCache(100);

    public static final Broker BROKER;

    static {
        BROKER = new Broker();
    }


    private Broker() {

    }

    public void init() {
        this.brokerInfo = new BrokerInfo(BROKER_CONFIG);
    }
}




