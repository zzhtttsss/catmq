package org.catmq.broker.manager;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.catmq.constant.FileConstant;
import org.catmq.constant.ZkConstant;
import org.catmq.entity.BooleanError;
import org.catmq.entity.JsonSerializable;
import org.catmq.entity.StorerInfo;
import org.catmq.entity.TopicDetail;
import org.catmq.util.Concat2String;
import org.catmq.util.StringUtil;
import org.catmq.zk.BaseZookeeper;

import java.util.*;

import static org.catmq.broker.Broker.BROKER;

@Slf4j
public class BrokerZkManager extends BaseZookeeper {
    @Getter
    private final String brokerPath;

    @Override
    public void register2Zk() {
        BooleanError res = registerBrokerInfo();
        if (!res.isSuccess()) {
            log.error("Register broker info to zk failed. {}", res.getError());
            System.exit(-1);
        }
        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            log.error("Thread sleep error.", e);
        }

    }

    @Override
    public void close() {
        super.client.close();
    }

    /**
     * This method is used to get all broker paths from zookeeper.
     *
     * @param isTmp true: get all broker paths from zookeeper,
     *              false: get all broker paths from tmp directory.
     * @return List<String> broker paths
     */
    public List<String> getAllBrokerPaths(boolean isTmp) {
        List<String> empty = new ArrayList<>();
        String path = ZkConstant.BROKER_ROOT_PATH;
        if (isTmp) {
            path = ZkConstant.TMP_BROKER_PATH;
        }
        try {
            return Optional.ofNullable(super.client.getChildren().forPath(path)).orElse(empty);
        } catch (Exception e) {
            return empty;
        }
    }

    /**
     * This method is used to register all broker info into zookeeper
     * whose path is /broker and the data is {@code BrokerInfo}.
     *
     * @return BooleanError
     */
    private BooleanError registerBrokerInfo() {
        try {
            if (this.client.checkExists().forPath(this.brokerPath) != null) {
                log.info("Broker info has been registered and will be deleted.");
                this.client.delete().deletingChildrenIfNeeded().forPath(this.brokerPath);
                log.info("Broker info has been deleted.");
            }
            this.client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(this.brokerPath, BROKER.getBrokerInfo().toBytes());
            this.client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(StringUtil.concatString(ZkConstant.TMP_BROKER_PATH, FileConstant.LEFT_SLASH, BROKER.getBrokerInfo().getBrokerAddress()));
        } catch (Exception e) {
            e.printStackTrace();
            return BooleanError.fail(e.getMessage());
        }
        return BooleanError.ok();
    }

    public void createPartition(TopicDetail topicDetail, String brokerPath) {
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(Concat2String.builder()
                            .concat(brokerPath)
                            .concat(FileConstant.LEFT_SLASH)
                            .concat(topicDetail.getTopicNameWithoutIndex())
                            .build(), brokerPath.getBytes());
            log.info("Create topic {} in zookeeper", topicDetail.getTopicNameWithoutIndex());
        } catch (Exception e) {
            log.warn("create topic {} failed", topicDetail.getTopicNameWithoutIndex(), e);
        }
    }

    public Optional<String[]> selectStorer(int num) {
        // TODO 选出messageLog最少的几个storer
        try {
            Map<String, Integer> map = new HashMap<>();

            List<String> paths = client.getChildren().forPath(ZkConstant.STORER_ROOT_PATH);
            String directory = StringUtil.concatString(ZkConstant.STORER_ROOT_PATH, FileConstant.LEFT_SLASH);
            for (String path : paths) {
                String fullPath = StringUtil.concatString(directory, path);
                byte[] bytes = client.getData().forPath(fullPath);
                StorerInfo info = JsonSerializable.fromBytes(bytes, StorerInfo.class);
                if (info == null) {
                    continue;
                }
                map.put(path, info.getMessageLogNum());
            }
            if (map.size() < num) {
                log.error("The number of brokers is less than the number of topics");
                return Optional.empty();
            }
            String[] storerZkPaths = map.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue())
                    .limit(num)
                    .map(Map.Entry::getKey)
                    .toArray(String[]::new);
            return Optional.of(storerZkPaths);

        } catch (Exception e) {
            log.error("Select broker error.", e);
            return Optional.empty();
        }
    }

    public BrokerZkManager(CuratorFramework client) {
        super(client);
        this.brokerPath = Concat2String
                .builder()
                .concat(ZkConstant.BROKER_ROOT_PATH)
                .concat(FileConstant.LEFT_SLASH)
                .concat(BROKER.getBrokerInfo().getBrokerAddress())
                .build();
    }

    public enum BrokerZkManagerEnum {
        INSTANCE;
        private final BrokerZkManager brokerZkManager;

        public BrokerZkManager getInstance() {
            return brokerZkManager;
        }

        BrokerZkManagerEnum() {
            brokerZkManager = new BrokerZkManager(BROKER.getClient());
        }
    }
}
