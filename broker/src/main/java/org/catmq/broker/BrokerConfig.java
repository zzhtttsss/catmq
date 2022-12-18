package org.catmq.broker;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

/**
 * @author BYL
 */
public class BrokerConfig {

    public final String BROKER_ID = UUID.randomUUID().toString();
    public String BROKER_NAME;
    public final String BROKER_IP;
    public final int BROKER_PORT;

    public BrokerConfig(String configPath) {
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
        BROKER_NAME = properties.getProperty("broker.name", "default");
        BROKER_IP = properties.getProperty("broker.ip", "127.0.0.1");
        BROKER_PORT = Integer.parseInt(properties.getProperty("broker.port", "8888"));
    }
}
