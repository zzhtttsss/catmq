package org.catmq.client;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.catmq.client.producer.ProducerProxy;
import org.catmq.entity.GrpcConnectManager;
import org.catmq.entity.TopicDetail;
import org.catmq.entity.TopicMode;
import org.catmq.entity.TopicType;
import org.catmq.constant.FileConstant;
import org.catmq.constant.ZkConstant;
import org.catmq.protocol.service.*;
import org.catmq.util.Concat2String;
import org.catmq.zk.TopicZkInfo;
import org.catmq.zk.ZkUtil;

import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.concurrent.*;

@Slf4j
public class CatClient {

    private final String tenantId;

    private CuratorFramework client;

    private ProducerProxy producerProxy;

    private final ThreadPoolExecutor producerHandleRequestExecutor;

    private final ThreadPoolExecutor producerHandleResponseExecutor;


    private final GrpcConnectManager grpcConnectManager;


    private CatClient(String zkAddress, String tenantId, ProducerProxy producerProxy) {
        this.client = ZkUtil.createClient(zkAddress);
        this.tenantId = tenantId;
        this.producerProxy = producerProxy;
        this.producerHandleRequestExecutor = new ThreadPoolExecutor(
                4,
                4,
                1,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(10000),
                new ThreadFactoryBuilder().setNameFormat("producerHandleRequestExecutor" + "-%d").build(),
                new ThreadPoolExecutor.DiscardOldestPolicy());

        this.producerHandleResponseExecutor = new ThreadPoolExecutor(
                4,
                4,
                1,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(10000),
                new ThreadFactoryBuilder().setNameFormat("producerHandleResponseExecutor" + "-%d").build(),
                new ThreadPoolExecutor.DiscardOldestPolicy());
        grpcConnectManager = new GrpcConnectManager(100);
    }

    public static CatClientBuilder builder() {
        return new CatClientBuilder();
    }

    public DefaultCatProducer.DefaultCatProducerBuilder createProducer() {
        return DefaultCatProducer.builder(tenantId, client, producerHandleRequestExecutor, producerHandleResponseExecutor,
                grpcConnectManager);
    }

    public void createSinglePartitionTopic(String topic, TopicType type, TopicMode mode) {
        createTopic(topic, type, mode, 1);
    }

    public void createTopic(String topic, TopicType type, TopicMode mode, int partitionNum) {
        try {
            String[] brokers = createTopicZkNode(topic, type, mode, partitionNum);
            createPartition(topic, type, mode, partitionNum, brokers);
        } catch (NoSuchElementException e) {
            log.error("no enough broker available");
        } catch (Exception e) {
            log.error("create topic {} failed", topic);
        }

    }

    public String[] createTopicZkNode(String topic, TopicType type, TopicMode mode, int partitionNum) throws Exception {
        String[] brokerZkPaths;
        brokerZkPaths = producerProxy.selectBrokers(client, partitionNum).orElseThrow();
        TopicDetail topicDetail = TopicDetail.get(type.getName(), mode.getName(), this.tenantId, topic);
        String TopicPath = Concat2String.builder()
                .concat(ZkConstant.TENANT_ROOT_PATH)
                .concat(FileConstant.LEFT_SLASH)
                .concat(tenantId)
                .concat(FileConstant.LEFT_SLASH)
                .concat(topicDetail.getCompleteTopicName())
                .build();

        HashMap<Integer, String> map = new HashMap<>(partitionNum);
        for (int i = 0; i < partitionNum; i++) {
            map.put(i, brokerZkPaths[i]);
        }
        TopicZkInfo info = new TopicZkInfo(topic, type.getName(), mode.getName(), partitionNum, map);
        client.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(TopicPath, info.toBytes());
        return brokerZkPaths;
    }

    public void createPartition(String topic, TopicType type, TopicMode mode, int partitionNum, String[] brokers) {
        for (int i = 0; i < partitionNum; i++) {
            Channel channel = grpcConnectManager.get(brokers[i]);
            Metadata metadata = new Metadata();
            metadata.put(Metadata.Key.of("action", Metadata.ASCII_STRING_MARSHALLER), "createPartition");
            metadata.put(Metadata.Key.of("tenant-id", Metadata.ASCII_STRING_MARSHALLER), tenantId);
            Channel headChannel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
            // TODO use async stub
            BrokerServiceGrpc.BrokerServiceBlockingStub stub = BrokerServiceGrpc.newBlockingStub(headChannel);

            CreatePartitionRequest request = CreatePartitionRequest.newBuilder()
                    .setTopic(topic)
                    .setPartitionIndex(i)
                    .setTopicType(type.getName())
                    .setTopicMode(mode.getName())
                    .build();
            CreatePartitionResponse response = stub.createPartition(request);
            // TODO handle response
            log.info("create partition {} in broker {}", topic, brokers[i]);
        }
    }



    public static class CatClientBuilder {

        private String zkAddress;

        private String tenantId;

        private ProducerProxy producerProxy;

        public CatClientBuilder setZkAddress(String zkAddress) {
            this.zkAddress = zkAddress;
            return this;
        }

        public CatClientBuilder setTenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public CatClientBuilder setProducerProxy(ProducerProxy producerProxy) {
            this.producerProxy = producerProxy;
            return this;
        }

        public CatClient build() {
            return new CatClient(zkAddress, tenantId, producerProxy);
        }
    }


}
