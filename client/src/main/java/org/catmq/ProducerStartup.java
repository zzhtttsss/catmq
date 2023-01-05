package org.catmq;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.catmq.constant.FileConstant;
import org.catmq.constant.ZkConstant;
import org.catmq.producer.ProducerConfig;
import org.catmq.util.StringUtil;
import org.catmq.zk.ZkUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class ProducerStartup {
    public static void main(String[] args) {
        ProducerConfig config = ProducerConfig.ProducerConfigEnum.INSTANCE.getInstance();
        config.readConfig("/producer.properties");
        String brokerAddress = getBrokerAddress(config.getZkAddress());
    }

    private static String getBrokerAddress(String zkAddress) {
        try (CuratorFramework client = ZkUtil.createClient(zkAddress)) {
            Map<String, Integer> map = new HashMap<>(4);
            try {
                List<String> paths = client.getChildren().forPath(ZkConstant.BROKER_ADDRESS);
                String addressDirectory = StringUtil.concatString(ZkConstant.BROKER_ADDRESS, FileConstant.LEFT_SLASH);
                for (String path : paths) {
                    byte[] bytes = client.getData().forPath(StringUtil.concatString(addressDirectory, path));
                    map.put(path, Integer.parseInt(new String(bytes)));
                }
                return map.entrySet().stream()
                        .min(Map.Entry.comparingByValue())
                        .map(stringIntegerEntry -> addressDirectory + stringIntegerEntry.getKey())
                        .orElseGet(() -> addressDirectory + paths.get(0));
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
