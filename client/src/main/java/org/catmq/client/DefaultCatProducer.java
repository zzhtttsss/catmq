package org.catmq.client;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.ByteString;
import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.catmq.client.common.ClientConfig;
import org.catmq.client.common.MessageEntry;
import org.catmq.client.common.PartitionSelector;
import org.catmq.client.producer.ProducerProxy;
import org.catmq.common.ConnectCache;
import org.catmq.common.TopicDetail;
import org.catmq.common.TopicType;
import org.catmq.protocol.definition.Message;
import org.catmq.protocol.definition.MessageType;
import org.catmq.protocol.service.*;
import org.catmq.zk.ZkUtil;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DefaultCatProducer extends ClientConfig {

    @Getter
    private final String tenantId;
    @Getter
    private final String producerGroup;
    @Getter
    private final TopicDetail topicDetail;
    @Getter
    private long producerId;
    @Getter
    private final CuratorFramework client;
    private PartitionSelector partitionSelector;

    private DefaultCatProducer(String tenantId, String producerGroup, String topic, CuratorFramework client,
                               PartitionSelector partitionSelector) {
        this.tenantId = tenantId;
        this.producerGroup = producerGroup;
        this.topicDetail = TopicDetail.get(topic);
        this.client = client;
        this.partitionSelector = partitionSelector;
        this.topicDetail.setBrokerAddress("127.0.0.1:5432");
        this.producerId = 1111L;
    }


    public void sendMessage(MessageEntry messageEntry) {
        // TODO 选择partition
        ManagedChannel channel = ConnectCache.get(topicDetail.getBrokerAddress());
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("action", Metadata.ASCII_STRING_MARSHALLER), "sendMessage");
        Channel headChannel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
        BrokerServiceGrpc.BrokerServiceBlockingStub blockingStub = BrokerServiceGrpc.newBlockingStub(headChannel);

        Message message = Message.newBuilder()
                .setTopic(topicDetail.getCompleteTopicName())
                .setBody(ByteString.copyFrom(messageEntry.getBody()))
                .setType(MessageType.NORMAL)
                .build();
        SendMessage2BrokerRequest request = SendMessage2BrokerRequest.newBuilder()
                .setMessage(message)
                .setProducerId(this.producerId)
                .build();
        SendMessage2BrokerResponse response;
        try {
            response = blockingStub.sendMessage2Broker(request);
        } catch (StatusRuntimeException e) {
            log.warn("RPC failed: {}", e.getMessage());
            return;
        }
        log.info("ack: {} \nresponse: {} \nstatus msg: {} \nstatus code: {}",
                response.getAck(), response.getRes(), response.getStatus().getMessage(),
                response.getStatus().getCode().getNumber());
    }


    public void sendMessage(Iterable<MessageEntry> messages) {
        for (MessageEntry messageEntry : messages) {
            sendMessage(messageEntry);
        }
    }

    public void AsyncSendMessage(MessageEntry messageEntry) {

    }


    protected static DefaultCatProducerBuilder builder(String tenantId, CuratorFramework client) {
        return new DefaultCatProducerBuilder(tenantId, client);
    }

    public static class DefaultCatProducerBuilder {
        private final String tenantId;
        private String producerGroup;

        private String topic;

        private CuratorFramework client;

        private PartitionSelector partitionSelector;

        protected DefaultCatProducerBuilder(String tenantId, CuratorFramework client) {
            this.tenantId = tenantId;
            this.client = client;
        }

        public DefaultCatProducerBuilder setProducerGroup(String producerGroup) {
            this.producerGroup = producerGroup;
            return this;
        }

        public DefaultCatProducerBuilder setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public DefaultCatProducerBuilder setZkAddress(CuratorFramework client) {
            this.client = client;
            return this;
        }



        public DefaultCatProducerBuilder setPartitionSelector(PartitionSelector partitionSelector) {
            this.partitionSelector = partitionSelector;
            return this;
        }

        public DefaultCatProducer build() {
            return new DefaultCatProducer(tenantId, producerGroup, topic, client, partitionSelector);
        }
    }


}
