package org.catmq.client.producer.balance;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.catmq.broker.BrokerInfo;
import org.catmq.constant.FileConstant;
import org.catmq.constant.ZkConstant;
import org.catmq.entity.JsonSerializable;
import org.catmq.util.StringUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class LeastUsedStrategy implements LoadBalance {
    @Override
    public Optional<String[]> selectBroker(CuratorFramework client, int num) {
        return selectBroker(client, num, null);
    }

    public Optional<String[]> selectBroker(CuratorFramework client, int num, String topic) {
        try {
            Map<String, Integer> map = new HashMap<>(4);

            List<String> paths = client.getChildren().forPath(ZkConstant.BROKER_ADDRESS_PATH);
            String addressDirectory = StringUtil.concatString(ZkConstant.BROKER_ADDRESS_PATH, FileConstant.LEFT_SLASH);
            for (String path : paths) {
                String fullPath = StringUtil.concatString(addressDirectory, path);
                byte[] bytes = client.getData().forPath(fullPath);
                BrokerInfo info = JsonSerializable.fromBytes(bytes, BrokerInfo.class);
                map.put(fullPath, info.getLoad());
            }
            if (map.size() < num) {
                log.error("The number of brokers is less than the number of topics");
                return Optional.empty();
            }
            String[] brokerZkPaths = map.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue())
                    .limit(num)
                    .map(stringIntegerEntry -> addressDirectory + stringIntegerEntry.getKey())
                    .toArray(String[]::new);
            return Optional.of(brokerZkPaths);

        } catch (Exception e) {
            log.error("Select broker error.", e);
            return Optional.empty();
        }
    }

    public enum LeastUsedStrategyEnum {
        /**
         * singleton
         */
        INSTANCE;

        private final LeastUsedStrategy leastUsedStrategy;

        LeastUsedStrategyEnum() {
            leastUsedStrategy = new LeastUsedStrategy();
        }

        public LeastUsedStrategy getStrategy() {
            return leastUsedStrategy;
        }
    }
}
