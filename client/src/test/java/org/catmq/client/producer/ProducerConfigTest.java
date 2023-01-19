package org.catmq.client.producer;

import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Properties;

public class ProducerConfigTest {

    @Test
    public void testReadConfig() {
        String path = "../conf/producer.properties";
        System.out.println(Path.of("..").toAbsolutePath().normalize().toAbsolutePath());
        String filePath = Path.of(path).toAbsolutePath().normalize().toString();
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(filePath)) {
            properties.load(inputStream);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(properties.getProperty("zk.address"));
    }
}
