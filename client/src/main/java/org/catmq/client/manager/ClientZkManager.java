package org.catmq.client.manager;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.catmq.client.producer.ProducerProxy;
import org.catmq.constant.FileConstant;
import org.catmq.constant.ZkConstant;
import org.catmq.entity.JsonSerializable;
import org.catmq.entity.TopicDetail;
import org.catmq.util.Concat2String;
import org.catmq.zk.BaseZookeeper;
import org.catmq.zk.TopicZkInfo;

import java.util.HashMap;
import java.util.Optional;

@Slf4j
public class ClientZkManager extends BaseZookeeper {
    @Override
    protected void register2Zk() {

    }

    @Override
    protected void close() {
        super.client.close();
    }

    public String[] createTopicZkNode(String tenantId, TopicDetail topicDetail,
                                      int partitionNum, ProducerProxy producerProxy) throws Exception {
        String[] brokerAddresses;
        brokerAddresses = producerProxy.selectBrokers(client, partitionNum).orElseThrow();
        String topicPath = Concat2String.builder()
                .concat(ZkConstant.TENANT_ROOT_PATH)
                .concat(FileConstant.LEFT_SLASH)
                .concat(tenantId)
                .concat(FileConstant.LEFT_SLASH)
                .concat(topicDetail.getTopicNameWithoutIndex())
                .build();
        log.info("topic path: {}", topicPath);

        HashMap<Integer, String> map = new HashMap<>(partitionNum);
        for (int i = 0; i < partitionNum; i++) {
            map.put(i, brokerAddresses[i]);
        }
        TopicZkInfo info = new TopicZkInfo(topicDetail.getSimpleName(), topicDetail.getType().getName(),
                topicDetail.getMode().getName(), partitionNum, map);
        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(topicPath, info.toBytes());
        log.info("create success");
        return brokerAddresses;
    }

    public Optional<TopicZkInfo> getTopicZkInfo(String tenantId, String topic) {
        String topicPath = Concat2String.builder()
                .concat(ZkConstant.TENANT_ROOT_PATH)
                .concat(FileConstant.LEFT_SLASH)
                .concat(tenantId)
                .concat(FileConstant.LEFT_SLASH)
                .concat(topic)
                .build();
        try {
            byte[] bytes = client.getData().forPath(topicPath);
            TopicZkInfo info = JsonSerializable.fromBytes(bytes, TopicZkInfo.class);
            return Optional.ofNullable(info);
        } catch (Exception e) {
            log.error("Fail to get broker list of this topic.", e);
            return Optional.empty();
        }
    }

    public ClientZkManager(CuratorFramework client) {
        super(client);
    }

}
