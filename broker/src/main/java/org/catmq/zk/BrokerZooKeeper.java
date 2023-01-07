package org.catmq.zk;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.zookeeper.CreateMode;
import org.catmq.broker.BrokerInfo;
import org.catmq.broker.BrokerServer;
import org.catmq.command.BooleanError;
import org.catmq.constant.FileConstant;
import org.catmq.constant.ZkConstant;
import org.catmq.util.StringUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author BYL
 */
@Slf4j
public class BrokerZooKeeper extends BaseZookeeper {
    @Getter
    private final BrokerServer broker;
    @Getter
    private final String brokerPath;

    @Override
    public void register2Zk() {
        BooleanError res = registerBrokerInfo();
        if (!res.isSuccess()) {
            log.error("Register broker info to zk failed. {}", res.getError());
            System.exit(-1);
        }
        res = registerBrokerConnection(this.broker);
        if (!res.isSuccess()) {
            log.error("Register broker address to zk failed. {}", res.getError());
            System.exit(-1);
        }

    }

    /**
     * Make it available to unit test
     *
     * @param path the path target client.
     */
    @Override
    public void increaseTheNumberOfRequestedSessions(String path) {
        super.increaseTheNumberOfRequestedSessions(path);
    }

    /**
     * Make it available to unit test
     *
     * @param path the path target client.
     */
    @Override
    public void decreaseTheNumberOfRequestedSessions(String path) {
        super.decreaseTheNumberOfRequestedSessions(path);
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
            this.client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(this.brokerPath, this.broker.brokerInfo.toBytes());
            this.client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(StringUtil.concatString(ZkConstant.TMP_BROKER_PATH, FileConstant.LEFT_SLASH, this.broker.brokerInfo.getBrokerName()));
        } catch (Exception e) {
            e.printStackTrace();
            return BooleanError.fail(e.getMessage());
        }
        return BooleanError.ok();
    }

    private BooleanError registerBrokerConnection(BrokerServer server) {
        BrokerInfo info = server.brokerInfo;
        CuratorFramework client = server.bzk.client;
        log.info("Register broker address to zk. {}", info.getBrokerIp() + FileConstant.Colon + info.getBrokerPort());
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(StringUtil.concatString(ZkConstant.BROKER_ADDRESS, FileConstant.LEFT_SLASH) +
                                    info.getBrokerIp() +
                                    ":" +
                                    info.getBrokerPort(),
                            "0".getBytes());
            CuratorCache cc = CuratorCache.build(client, ZkConstant.BROKER_ADDRESS);
            cc.listenable().addListener(new DeadNodeListener(info));
            cc.start();
        } catch (Exception e) {
            log.error("Register broker address to zk failed. {}", e.getMessage());
            return BooleanError.fail(e.getMessage());
        }

        return BooleanError.ok();
    }

    public BrokerZooKeeper(String host, BrokerServer broker) {
        super(host);
        this.broker = broker;
        this.brokerPath = String.format("/broker/%s", this.broker.brokerInfo.getBrokerName());
        this.client.start();
    }
}
