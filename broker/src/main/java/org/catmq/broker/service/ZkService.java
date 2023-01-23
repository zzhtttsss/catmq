package org.catmq.broker.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.CreateMode;
import org.catmq.broker.BrokerConfig;
import org.catmq.constant.FileConstant;
import org.catmq.constant.ZkConstant;
import org.catmq.util.Concat2String;

import java.io.Closeable;
import java.io.IOException;

import static org.catmq.broker.BrokerConfig.BROKER_CONFIG;

@Slf4j
public class ZkService implements Closeable {
    private final CuratorFramework client;

    public void createTopic(String completeTopic, String brokerPath) {
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(Concat2String.builder()
                            .concat(ZkConstant.TOPIC_ROOT_PATH)
                            .concat(FileConstant.LEFT_SLASH)
                            .concat(completeTopic)
                            .build(), brokerPath.getBytes());
            log.info("Create topic {} in zookeeper", completeTopic);
        } catch (Exception e) {
            e.printStackTrace();
            log.warn("create topic {} failed", completeTopic);
        }
    }

    public void createPartition(String completeTopic, String brokerPath) {
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(Concat2String.builder()
                            .concat(ZkConstant.TOPIC_ROOT_PATH)
                            .concat(FileConstant.LEFT_SLASH)
                            .concat(completeTopic)
                            .build(), brokerPath.getBytes());
            log.info("Create topic {} in zookeeper", completeTopic);
        } catch (Exception e) {
            e.printStackTrace();
            log.warn("create topic {} failed", completeTopic);
        }
    }


    @Override
    public void close() throws IOException {
        client.close();
    }

    private ZkService() {
        this.client = CuratorFrameworkFactory.newClient(BROKER_CONFIG.getZkAddress(), new RetryOneTime(1000));
        client.start();
    }

    public enum ZkServiceEnum {
        INSTANCE;
        private final ZkService zkService;

        public ZkService getInstance() {
            return zkService;
        }

        ZkServiceEnum() {
            zkService = new ZkService();
        }
    }
}
