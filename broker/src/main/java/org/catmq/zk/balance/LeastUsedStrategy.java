package org.catmq.zk.balance;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.zookeeper.CreateMode;
import org.catmq.broker.BrokerInfo;
import org.catmq.command.BooleanError;
import org.catmq.constant.FileConstant;
import org.catmq.zk.DeadNodeListener;
import org.catmq.zk.ZkUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class LeastUsedStrategy implements ILoadBalance {
    @Override
    public BooleanError registerConnection(BrokerInfo info) {
        try (CuratorFramework client = ZkUtil.createClient(info.getZkAddress())) {
            log.info("Register broker address to zk. {}", info.getBrokerIp() + FileConstant.Colon + info.getBrokerPort());
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath("/address/broker/" +
                                    info.getBrokerIp() +
                                    ":" +
                                    info.getBrokerPort(),
                            "0".getBytes());
            CuratorCache cc = CuratorCache.build(client, "/address/broker");
            cc.listenable().addListener(new DeadNodeListener(info));
            cc.start();
        } catch (Exception e) {
            e.printStackTrace();
            return BooleanError.fail(e.getMessage());
        }
        return BooleanError.ok();
    }

    @Override
    public String getOptimalConnection() {
        try (CuratorFramework client = ZkUtil.createClient("127.0.0.1:2181")) {
            Map<String, Integer> map = new HashMap<>(4);
            try {
                List<String> paths = client.getChildren().forPath("/address/broker");
                for (String path : paths) {
                    byte[] bytes = client.getData().forPath("/address/broker/" + path);
                    map.put(path, Integer.parseInt(new String(bytes)));
                }
                return map.entrySet().stream()
                        .min(Map.Entry.comparingByValue())
                        .map(stringIntegerEntry -> "/address/broker/" + stringIntegerEntry.getKey())
                        .orElseGet(() -> "/address/broker/" + paths.get(0));
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
