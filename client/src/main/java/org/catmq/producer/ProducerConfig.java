package org.catmq.producer;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.catmq.constant.ConfigConstant;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Setter
@Getter
@RequiredArgsConstructor
public class ProducerConfig {
    private String zkAddress;

    public void readConfig(String configPath) {
        InputStream stream = this.getClass().getResourceAsStream(configPath);
        if (stream == null) {
            throw new RuntimeException("producer.properties not found");
        }
        Properties properties = new Properties();
        try {
            properties.load(stream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        zkAddress = properties.getProperty(ConfigConstant.ZK_ADDRESS, "127.0.0.1:2181");
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
