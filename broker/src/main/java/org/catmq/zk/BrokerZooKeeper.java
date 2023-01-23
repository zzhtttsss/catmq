package org.catmq.zk;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.catmq.broker.BrokerInfo;
import org.catmq.broker.BrokerServer;
import org.catmq.command.BooleanError;
import org.catmq.constant.FileConstant;
import org.catmq.constant.ZkConstant;
import org.catmq.util.Concat2String;
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
        try {
            Thread.sleep(1000);
        } catch (Exception e) {
            log.error("Thread sleep error. {}", e.getMessage());
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
                this.client.delete().forPath(this.brokerPath);
                log.info("Broker info has been deleted.");
            }
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
        log.info("Register broker address to zk. {}", info.getBrokerIp() + FileConstant.COLON + info.getBrokerPort());
        try {
            // /address/broker/127.0.0.1:5432
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(Concat2String.builder()
                            .concat(ZkConstant.BROKER_ADDRESS_PATH)
                            .concat(FileConstant.LEFT_SLASH)
                            .concat(info.getBrokerIp())
                            .concat(FileConstant.COLON)
                            .concat(info.getBrokerPort())
                            .build(), info.toBytes());
            /*CuratorCache cc = CuratorCache.build(client, ZkConstant.BROKER_ADDRESS_PATH);
            cc.listenable().addListener(new DeadNodeListener(info));
            cc.start();*/
        } catch (Exception e) {
            return BooleanError.fail(e.getMessage());
        }

        return BooleanError.ok();
    }

    public BrokerZooKeeper(String host, BrokerServer broker) {
        super(host);
        this.broker = broker;
        this.brokerPath = String.format("/broker/%s", this.broker.brokerInfo.getBrokerId());
    }
}
