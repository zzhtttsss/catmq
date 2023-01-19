package org.catmq.client.consumer;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.constant.ConfigConstant;
import org.catmq.constant.ZkConstant;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Properties;

@Setter
@Getter
@Slf4j
public class ConsumerConfig {
    private String zkAddress;
    /**
     * broker ip and port without clusters which
     * <strong>should be specified manually</strong>
     */
    private InetSocketAddress brokerAddress;

    private ConsumerConfig() {
        String filePath = Path.of(ConfigConstant.CONSUMER_CONFIG_PATH).toAbsolutePath().normalize().toString();
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(filePath)) {
            properties.load(inputStream);
        } catch (Exception e) {
            e.printStackTrace();
            log.warn("read config failed, use default config");
        }
        zkAddress = properties.getProperty(ConfigConstant.ZK_ADDRESS, ZkConstant.ZK_DEFAULT_ADDRESS);
    }

    public enum ConsumerConfigEnum {
        /**
         * Singleton
         */
        INSTANCE;
        private final ConsumerConfig consumerConfig;

        ConsumerConfigEnum() {
            consumerConfig = new ConsumerConfig();
        }

        public ConsumerConfig getInstance() {
            return consumerConfig;
        }
    }
}
