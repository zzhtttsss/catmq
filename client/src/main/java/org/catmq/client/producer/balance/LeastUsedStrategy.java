package org.catmq.client.producer.balance;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.catmq.broker.BrokerInfo;
import org.catmq.constant.FileConstant;
import org.catmq.constant.ZkConstant;
import org.catmq.entity.ISerialization;
import org.catmq.util.StringUtil;
import org.catmq.zk.ZkUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Deprecated
public class LeastUsedStrategy implements ILoadBalance {
    @Override
    public Optional<String> selectBroker(String zkAddress) {
        try (CuratorFramework client = ZkUtil.createClient(zkAddress)) {
            Map<String, Integer> map = new HashMap<>(4);

            List<String> paths = client.getChildren().forPath(ZkConstant.BROKER_ADDRESS_PATH);
            String addressDirectory = StringUtil.concatString(ZkConstant.BROKER_ADDRESS_PATH, FileConstant.LEFT_SLASH);
            for (String path : paths) {
                byte[] bytes = client.getData().forPath(StringUtil.concatString(addressDirectory, path));
                BrokerInfo info = ISerialization.fromBytes(bytes, BrokerInfo.class);
                map.put(path, info.getLoad());
            }

            return map.entrySet().stream()
                    .min(Map.Entry.comparingByValue())
                    .map(stringIntegerEntry -> addressDirectory + stringIntegerEntry.getKey())
                    .orElseGet(() -> addressDirectory + paths.get(0))
                    .describeConstable();
        } catch (Exception e) {
            e.printStackTrace();
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
