package org.catmq.broker.zk;

import lombok.Getter;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.catmq.Broker;
import org.catmq.command.BooleanError;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author BYL
 */
public class BaseZooKeeper {

    @Getter
    private Broker broker;
    public final CuratorFramework client;
    @Getter
    private final String brokerPath;

    /**
     * This method is used to register the broker to zookeeper which will store {@code BrokerInfo}.
     * Note: <strong>The CreateMode.EPHEMERAL is used in dev environment</strong>
     *
     * @return BooleanError
     */
    public BooleanError register2Zk() {
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
            return Optional.ofNullable(this.client.getChildren().forPath(path)).orElse(empty);
        } catch (Exception e) {
            return empty;
        }
    }

    private void initWatcher() {

    }


    public BaseZooKeeper(String host, Broker broker) throws IOException {
        this.broker = broker;
        RetryPolicy policy = new ExponentialBackoffRetry(1000, 3);
        this.client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", policy);
        this.brokerPath = String.format("/broker/%s", this.broker.brokerInfo.getBrokerName());
        this.client.start();
    }
}
