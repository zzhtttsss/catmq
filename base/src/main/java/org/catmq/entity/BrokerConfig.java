package org.catmq.entity;

import lombok.Getter;
import lombok.Setter;
import org.catmq.constant.ConfigConstant;
import org.catmq.constant.ZkConstant;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.catmq.util.ConfigUtil.PROCESSOR_NUMBER;

@Setter
@Getter
public class BrokerConfig {

    public static final BrokerConfig BROKER_CONFIG;

    static {
        BROKER_CONFIG = new BrokerConfig();
    }

    private long brokerId;
    private String brokerName;
    private String brokerIp;
    private String zkAddress;
    private int brokerPort = 5432;
    private int grpcProducerThreadQueueCapacity = 10000;
    private int grpcProducerThreadPoolNums = PROCESSOR_NUMBER;
    private int grpcAdminThreadPoolNums = 1;
    private int maxSegmentMessageNum;


    private BrokerConfig() {
        InputStream stream = this.getClass().getResourceAsStream(ConfigConstant.BROKER_CONFIG_PATH);
        if (stream == null) {
            throw new RuntimeException("broker.properties not found");
        }
        Properties properties = new Properties();
        try {
            properties.load(stream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        brokerName = properties.getProperty(ConfigConstant.BROKER_NAME, "default");
        brokerIp = properties.getProperty(ConfigConstant.BROKER_IP, "127.0.0.1");
        brokerPort = Integer.parseInt(properties.getProperty(ConfigConstant.BROKER_PORT, String.valueOf(brokerPort)));
        grpcProducerThreadQueueCapacity = Integer.parseInt(properties.getProperty(ConfigConstant.GRPC_PRODUCER_THREAD_QUEUE_CAPACITY, String.valueOf(grpcProducerThreadQueueCapacity)));
        grpcProducerThreadPoolNums = Integer.parseInt(properties.getProperty(ConfigConstant.GRPC_PRODUCER_THREAD_POOL_NUMS, String.valueOf(grpcProducerThreadPoolNums)));
        zkAddress = properties.getProperty(ConfigConstant.ZK_ADDRESS, ZkConstant.ZK_DEFAULT_ADDRESS);
        maxSegmentMessageNum = Integer.parseInt(properties.getProperty(ConfigConstant.TOPIC_MAX_SEGMENT_MESSAGE_NUM, String.valueOf(10000)));
    }
}
