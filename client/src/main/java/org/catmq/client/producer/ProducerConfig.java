package org.catmq.client.producer;

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
public class ProducerConfig {
    private String zkAddress;
    /**
     * broker ip and port without clusters which
     * <strong>should be specified manually</strong>
     */
    private InetSocketAddress brokerAddress;

    private ProducerConfig() {
        String filePath = Path.of(ConfigConstant.PRODUCER_CONFIG_PATH).toAbsolutePath().normalize().toString();
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(filePath)) {
            properties.load(inputStream);
        } catch (Exception e) {
            e.printStackTrace();
            log.warn("read config failed, use default config");
        }
        zkAddress = properties.getProperty(ConfigConstant.ZK_ADDRESS, ZkConstant.ZK_DEFAULT_ADDRESS);
    }

    public enum ProducerConfigEnum {
        /**
         * Singleton
         */
        INSTANCE;
        private final ProducerConfig producerConfig;


        ProducerConfigEnum() {
            producerConfig = new ProducerConfig();
        }

        public ProducerConfig getInstance() {
            return producerConfig;
        }
    }
}
