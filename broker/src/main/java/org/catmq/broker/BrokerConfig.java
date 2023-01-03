package org.catmq.broker;

import lombok.Getter;
import lombok.Setter;
import org.catmq.constant.ConfigConstant;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

import static org.catmq.util.ConfigUtil.PROCESSOR_NUMBER;

@Setter
@Getter
public class BrokerConfig {

    private String brokerId = UUID.randomUUID().toString();
    private String brokerName;
    private String brokerIp;
    private int brokerPort = 5432;
    private int grpcProducerThreadQueueCapacity = 10000;
    private int grpcProducerThreadPoolNums = PROCESSOR_NUMBER;

    public void readConfig(String configPath) {
        InputStream stream = this.getClass().getResourceAsStream(configPath);
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
    }

    private BrokerConfig() {
    }
    
    public enum BrokerConfigEnum {
        /**
         * Singleton
         */
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
