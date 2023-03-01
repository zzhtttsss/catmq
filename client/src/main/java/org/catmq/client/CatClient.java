package org.catmq.client;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.catmq.client.manager.ClientZkManager;
import org.catmq.client.producer.ProducerProxy;
import org.catmq.entity.GrpcConnectManager;
import org.catmq.entity.TopicDetail;
import org.catmq.protocol.definition.Code;
import org.catmq.protocol.service.BrokerServiceGrpc;
import org.catmq.protocol.service.CreatePartitionRequest;
import org.catmq.protocol.service.CreatePartitionResponse;
import org.catmq.util.StringUtil;
import org.catmq.zk.TopicZkInfo;
import org.catmq.zk.ZkUtil;

import java.io.Closeable;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.catmq.entity.TopicDetail.PARTITIONED_INDEX_SEPARATOR;
import static org.catmq.util.ConfigUtil.PROCESSOR_NUMBER;

@Slf4j
public class CatClient implements Closeable {

    private final String tenantId;

    private final CuratorFramework client;

    private ClientZkManager clientZkManager;

    private ProducerProxy producerProxy;

    private final ThreadPoolExecutor clientHandleRequestExecutor;

    private final ThreadPoolExecutor clientHandleResponseExecutor;


    private final GrpcConnectManager grpcConnectManager;

    /**
     * topic <strong>complete</strong> name -> broker address list
     */
    protected final LoadingCache<String, TopicZkInfo> topicBrokerCache = CacheBuilder
            .newBuilder()
            .maximumSize(100)
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .build(new CacheLoader<>() {
                @Override
                public @NonNull TopicZkInfo load(@NonNull String topic) {
                    return clientZkManager.getTopicZkInfo(tenantId, topic).orElseThrow();
                }
            });

    private CatClient(String zkAddress, String tenantId, ProducerProxy producerProxy) {
        this.client = ZkUtil.createClient(zkAddress);
        this.clientZkManager = new ClientZkManager(this.client);
        this.tenantId = tenantId;
        this.producerProxy = producerProxy;
        this.clientHandleRequestExecutor = new ThreadPoolExecutor(
                PROCESSOR_NUMBER,
                PROCESSOR_NUMBER,
                1,
                TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(10000),
                new ThreadFactoryBuilder().setNameFormat("producerHandleRequestExecutor" + "-%d").build(),
                new ThreadPoolExecutor.DiscardOldestPolicy());

        this.clientHandleResponseExecutor = new ThreadPoolExecutor(
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

    public DefaultCatProducer.DefaultCatProducerBuilder createProducer(String completeTopic) {
        try {
            return DefaultCatProducer.builder(tenantId, client, clientHandleRequestExecutor,
                    clientHandleResponseExecutor, grpcConnectManager, completeTopic,
                    topicBrokerCache.get(completeTopic));
        } catch (ExecutionException e) {
            log.warn("Fail to get broker list of topic.", e);
            throw new RuntimeException(e);
        }

    }

    public DefaultCatConsumer.DefaultCatConsumerBuilder createConsumer(String topic, String subscription) {
        try {
            return DefaultCatConsumer.builder()
                    .setTenantId(tenantId)
                    .setClient(this)
                    .setTopicDetail(TopicDetail.get(topic))
                    .setSubscriptionName(subscription)
                    .setGrpcConnectManager(grpcConnectManager)
                    .setHandleRequestExecutor(clientHandleRequestExecutor)
                    .setHandleResponseExecutor(clientHandleResponseExecutor)
                    .setTopicZkInfo(topicBrokerCache.get(topic));
        } catch (ExecutionException e) {
            log.warn("Fail to get broker list of topic.", e);
            throw new RuntimeException(e);
        }
    }

    public void createSinglePartitionTopic(String topic, String type, String mode) {
        createTopic(topic, type, mode, 1);
    }

    public void createTopic(String topic, String type, String mode, int partitionNum) {
        TopicDetail topicDetail = TopicDetail.get(type, mode, this.tenantId, topic);
        try {
            String[] brokers = clientZkManager.createTopicZkNode(tenantId, topicDetail, partitionNum, producerProxy);
            createPartition(topicDetail, partitionNum, brokers);
        } catch (NoSuchElementException e) {
            log.error("no enough broker available");
        } catch (Exception e) {
            log.error("create topic {} failed", topic, e);
        }

    }

    public void createPartition(TopicDetail topicDetail, int partitionNum, String[] brokers) {
        for (int i = 0; i < partitionNum; i++) {
            Channel channel = grpcConnectManager.get(brokers[i]);
            Metadata metadata = new Metadata();
            metadata.put(Metadata.Key.of("action", Metadata.ASCII_STRING_MARSHALLER), "createPartition");
            metadata.put(Metadata.Key.of("tenant-id", Metadata.ASCII_STRING_MARSHALLER), tenantId);
            Channel headChannel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
            // TODO use async stub
            BrokerServiceGrpc.BrokerServiceBlockingStub stub = BrokerServiceGrpc.newBlockingStub(headChannel);

            CreatePartitionRequest request = CreatePartitionRequest.newBuilder()
                    .setTopic(StringUtil.concatString(topicDetail.getTopicNameWithoutIndex(),
                            PARTITIONED_INDEX_SEPARATOR, String.valueOf(i)))
                    .setTopicType(topicDetail.getType().getName())
                    .setTopicMode(topicDetail.getMode().getName())
                    .build();
            CreatePartitionResponse response = stub.createPartition(request);
            if (response.getStatus().getCode() != Code.OK) {

            } else {
                log.info("create partition {} in broker {}", topicDetail.getCompleteTopicName(), brokers[i]);
            }
        }
    }

    @Override
    public void close() {
        clientHandleRequestExecutor.shutdown();
        clientHandleResponseExecutor.shutdown();
        client.close();
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
