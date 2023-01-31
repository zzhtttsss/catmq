package org.catmq.client.producer;

import lombok.extern.slf4j.Slf4j;
import org.catmq.constant.FileConstant;
import org.catmq.constant.ZkConstant;
import org.catmq.entity.BrokerInfo;
import org.catmq.entity.JsonSerializable;
import org.catmq.entity.TopicDetail;
import org.catmq.util.Concat2String;
import org.catmq.zk.BaseZookeeper;
import org.catmq.zk.ZkUtil;

@Slf4j
public class ProducerZooKeeper extends BaseZookeeper {

    private final ProducerConfig config;
    /**
     * broker info where this producer connected
     */
    private final BrokerInfo brokerInfo;

    public boolean checkTopicExists(TopicDetail topicDetail) {
        String brokerPath = Concat2String.builder()
                .concat(ZkConstant.BROKER_ROOT_PATH)
                .concat(FileConstant.LEFT_SLASH)
                .concat(brokerInfo.getBrokerName())
                .concat(FileConstant.LEFT_SLASH)
                .concat(topicDetail.getTenant())
                .concat(FileConstant.LEFT_SLASH)
                .concat(topicDetail.getSimpleName())
                .build();
        try {
            return client.checkExists().forPath(brokerPath) != null;
        } catch (Exception e) {
            log.error("check topic exists error", e);
            return false;
        }
    }


    @Override
    public void register2Zk() {
        String addressPath = ZkUtil.getFullBrokerAddressPath(config.getBrokerAddress());
        log.info("Path [{}] load increase", addressPath);
        increaseTheNumberOfRequestedSessions(addressPath);
    }

    @Override
    public void close() {
        String addressPath = ZkUtil.getFullBrokerAddressPath(config.getBrokerAddress());
        log.info("Path [{}] load decrease", addressPath);
        decreaseTheNumberOfRequestedSessions(addressPath);
        super.client.close();
    }

    /**
     * This method is used to increase the number of connections on specified client.
     *
     * @param path the path target client like /address/broker/ip:port.
     */
    protected void increaseTheNumberOfRequestedSessions(String path) {
        try {
            // broker info may be outdated, so we need to get the latest broker info from zookeeper
            BrokerInfo info = getBrokerInfo();
            info.setLoad(info.getLoad() + 1);
            client.setData().forPath(path, info.toBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This method is used to decrease the number of connections on specified client.
     *
     * @param path the path target client like /address/broker/ip:port.
     */
    protected void decreaseTheNumberOfRequestedSessions(String path) {
        try {
            BrokerInfo info = getBrokerInfo();
            if (info.getLoad() <= 0) {
                log.error("The number of connections on {} is less than 0.", path);
                return;
            }
            info.setLoad(info.getLoad() - 1);
            client.setData().forPath(path, info.toBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Get broker information where this producer connected from zk.
     * <strong>One producer connects with many brokers which has not been considered yet</strong>
     */
    private BrokerInfo getBrokerInfo() {
        String path = ZkUtil.getFullBrokerAddressPath(config.getBrokerAddress());
        try {
            byte[] bytes = client.getData().forPath(path);
            return JsonSerializable.fromBytes(bytes, BrokerInfo.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public ProducerZooKeeper(ProducerConfig config) {
        super(null);
        this.config = config;
        this.brokerInfo = getBrokerInfo();
    }


}
