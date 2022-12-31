package org.catmq.zk;

import lombok.Getter;
import org.apache.zookeeper.CreateMode;
import org.catmq.Broker;
import org.catmq.command.BooleanError;
import org.catmq.zk.balance.ILoadBalance;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author BYL
 */
public class BrokerZooKeeper extends BaseZookeeper {
    private final Logger log = org.slf4j.LoggerFactory.getLogger(BrokerZooKeeper.class);

    @Getter
    private final Broker broker;
    @Getter
    private final String brokerPath;

    private final ILoadBalance balanceStrategy;

    @Override
    public void register2Zk() {
        BooleanError res = registerBrokerInfo();
        if (!res.isSuccess()) {
            log.error("Register broker info to zk failed. {}", res.getError());
            System.exit(-1);
        }
        res = this.balanceStrategy.registerConnection(this.broker.brokerInfo);
        if (!res.isSuccess()) {
            log.error("Register broker address to zk failed. {}", res.getError());
            System.exit(-1);
        }
    }

    @Override
    public String getOptimalConnection() {
        String target = this.balanceStrategy.getOptimalConnection();
        if (target == null) {
            return this.broker.brokerInfo.getBrokerIp() + ":" + this.broker.brokerInfo.getBrokerPort();
        }
        return target;
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
     * @param isNotTmp true: get all broker paths from zookeeper,
     *                 false: get all broker paths from tmp directory.
     * @return List<String> broker paths
     */
    public List<String> getAllBrokerPaths(boolean isNotTmp) {
        List<String> empty = new ArrayList<>();
        String path = "/broker";
        if (!isNotTmp) {
            path = "/broker/tmp";
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
                    .forPath("/broker/tmp/" + this.broker.brokerInfo.getBrokerName());
        } catch (Exception e) {
            e.printStackTrace();
            return BooleanError.fail(e.getMessage());
        }
        return BooleanError.ok();
    }
    
    public BrokerZooKeeper(String host, Broker broker, ILoadBalance balanceStrategy) throws IOException {
        super(host);
        this.broker = broker;
        this.brokerPath = String.format("/broker/%s", this.broker.brokerInfo.getBrokerName());
        this.balanceStrategy = balanceStrategy;
        this.client.start();
    }
}
