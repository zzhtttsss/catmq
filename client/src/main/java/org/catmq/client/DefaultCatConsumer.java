package org.catmq.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.client.common.ClientConfig;
import org.catmq.client.common.ConsumeCallback;
import org.catmq.client.common.PartitionSelector;
import org.catmq.entity.ConsumerBatchPolicy;
import org.catmq.entity.GrpcConnectManager;
import org.catmq.entity.TopicDetail;
import org.catmq.protocol.definition.ProcessMode;
import org.catmq.protocol.service.*;
import org.catmq.thread.ThreadPoolMonitor;
import org.catmq.util.StringUtil;
import org.catmq.zk.TopicZkInfo;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Slf4j
@Getter
public class DefaultCatConsumer extends ClientConfig {

    private String tenantId;
    private CatClient client;

    private long consumerId;

    private ConsumerBatchPolicy batchPolicy;

    private String subscriptionName;
    private ThreadPoolExecutor handleResponseExecutor;

    private ThreadPoolExecutor handleRequestExecutor;

    private GrpcConnectManager grpcConnectManager;

    private TopicDetail topicDetail;

    private PartitionSelector partitionSelector;

    private TopicZkInfo topicZkInfo;

    public void subscribe() {
        TopicDetail topicDetail = TopicDetail.get(topicZkInfo.getType(), topicZkInfo.getMode(),
                this.tenantId, topicZkInfo.getSimpleName());
        ManagedChannel channel = grpcConnectManager.get(topicZkInfo.getBrokerZkPaths().get(0));
        doSubscribe(topicDetail, channel);
        log.info("subscribe topic {} success", topicDetail.getCompleteTopicName());
    }

    public void getMessage(Consumer<GetMessageFromBrokerResponse> consumer, int timeout, TimeUnit timeUnit) {
        ManagedChannel channel = grpcConnectManager.get(topicZkInfo.getBrokerZkPaths().get(0));
        doAsyncGetMessage(channel, ProcessMode.SYNC, consumer, null, timeout, timeUnit);
    }

    public void getAsyncMessage(ConsumeCallback<GetMessageFromBrokerResponse> callback, int timeout, TimeUnit timeUnit) {
        ManagedChannel channel = grpcConnectManager.get(topicZkInfo.getBrokerZkPaths().get(0));
        doAsyncGetMessage(channel, ProcessMode.ASYNC, null, callback, timeout, timeUnit);
    }

    @VisibleForTesting
    void doSubscribe(TopicDetail topicDetail, ManagedChannel channel) {
        // broker 1:1 consumer
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("action", Metadata.ASCII_STRING_MARSHALLER), "subscribe");
        metadata.put(Metadata.Key.of("tenant-id", Metadata.ASCII_STRING_MARSHALLER), tenantId);
        Channel headChannel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
        // use sync stub to ensure that the subscribe request is sent to the broker
        BrokerServiceGrpc.BrokerServiceBlockingStub stub = BrokerServiceGrpc.newBlockingStub(headChannel);
        SubscribeRequest subscribeRequest = SubscribeRequest.newBuilder()
                .setTopic(topicDetail.getCompleteTopicName() + "#0")
                .setSubscription(this.subscriptionName)
                .setConsumerId(consumerId)
                .build();
        SubscribeResponse response = stub.subscribe(subscribeRequest);
        System.out.println(response);
    }

    @VisibleForTesting
    void doAsyncGetMessage(ManagedChannel channel, ProcessMode processMode,
                           final Consumer<GetMessageFromBrokerResponse> consumer,
                           final ConsumeCallback<GetMessageFromBrokerResponse> callback,
                           final long timeout, final TimeUnit timeUnit) {
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("action", Metadata.ASCII_STRING_MARSHALLER), "getMessage");
        metadata.put(Metadata.Key.of("tenant-id", Metadata.ASCII_STRING_MARSHALLER), tenantId);
        Channel headChannel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
        var futureStub = BrokerServiceGrpc.newFutureStub(headChannel);
        GetMessageFromBrokerRequest request = GetMessageFromBrokerRequest.newBuilder()
                .setTopic(topicDetail.getCompleteTopicName() + "#0")
                .setSubscription(subscriptionName)
                .setConsumerId(consumerId)
                .setBatchNumber(batchPolicy.getBatchNumber())
                .setTimeoutInMs(batchPolicy.getTimeoutInMs())
                .build();
        ListenableFuture<GetMessageFromBrokerResponse> responseFuture = futureStub.getMessageFromBroker(request);

        switch (processMode) {
            case SYNC -> {
                try {
                    GetMessageFromBrokerResponse response = responseFuture.get(timeout, timeUnit);
                    consumer.accept(response);
                } catch (Exception e) {
                    log.error("get message from broker error", e);
                }
            }
            case ASYNC -> {
                Futures.addCallback(responseFuture, new FutureCallback<>() {
                    @Override
                    public void onSuccess(GetMessageFromBrokerResponse result) {
                        callback.onSuccess(result);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        callback.onException(t);
                    }
                }, MoreExecutors.directExecutor());
            }
            default -> log.error("Unknown process mode: {}", processMode);
        }

    }


    private DefaultCatConsumer(String tenantId, String topic, CatClient client, String subscriptionName,
                               PartitionSelector partitionSelector, ThreadPoolExecutor handleResponseExecutor,
                               ThreadPoolExecutor handleRequestExecutor, GrpcConnectManager grpcConnectManager,
                               TopicZkInfo topicZkInfo, long consumerId, ConsumerBatchPolicy policy) {
        this.tenantId = tenantId;
        this.topicDetail = TopicDetail.get(topic);
        this.client = client;
        this.subscriptionName = subscriptionName;
        this.partitionSelector = partitionSelector;
        this.consumerId = consumerId;
        this.handleRequestExecutor = handleRequestExecutor;
        this.handleResponseExecutor = handleResponseExecutor;
        this.grpcConnectManager = grpcConnectManager;
        this.topicZkInfo = topicZkInfo;
        this.batchPolicy = policy;
    }

    public static DefaultCatConsumerBuilder builder() {
        return new DefaultCatConsumerBuilder();
    }

    public static class DefaultCatConsumerBuilder {
        private String tenantId;
        private CatClient client;
        private long consumerId;
        private ConsumerBatchPolicy batchPolicy = ConsumerBatchPolicy.ConsumerBatchPolicyBuilder
                .builder()
                .setBatchNumber(10)
                .setTimeoutInMs(500)
                .build();
        private String subscriptionName;
        private ThreadPoolExecutor handleResponseExecutor;
        private ThreadPoolExecutor handleRequestExecutor;
        private GrpcConnectManager grpcConnectManager;
        private TopicDetail topicDetail;
        private PartitionSelector partitionSelector;
        private TopicZkInfo topicZkInfo;


        public DefaultCatConsumerBuilder setTenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public DefaultCatConsumerBuilder setClient(CatClient client) {
            this.client = client;
            return this;
        }

        public DefaultCatConsumerBuilder setConsumerId(long consumerId) {
            this.consumerId = consumerId;
            return this;
        }

        public DefaultCatConsumerBuilder setBatchPolicy(ConsumerBatchPolicy batchPolicy) {
            this.batchPolicy = batchPolicy;
            return this;
        }

        public DefaultCatConsumerBuilder setTopicDetail(TopicDetail topicDetail) {
            this.topicDetail = topicDetail;
            return this;
        }

        public DefaultCatConsumerBuilder setPartitionSelector(PartitionSelector partitionSelector) {
            this.partitionSelector = partitionSelector;
            return this;
        }

        public DefaultCatConsumerBuilder setTopicZkInfo(TopicZkInfo topicZkInfo) {
            this.topicZkInfo = topicZkInfo;
            return this;
        }

        public DefaultCatConsumerBuilder setGrpcConnectManager(GrpcConnectManager grpcConnectManager) {
            this.grpcConnectManager = grpcConnectManager;
            return this;
        }

        public DefaultCatConsumerBuilder setHandleRequestExecutor(ThreadPoolExecutor handleRequestExecutor) {
            this.handleRequestExecutor = handleRequestExecutor;
            return this;
        }

        public DefaultCatConsumerBuilder setHandleResponseExecutor(ThreadPoolExecutor handleResponseExecutor) {
            this.handleResponseExecutor = handleResponseExecutor;
            return this;
        }

        public DefaultCatConsumerBuilder setSubscriptionName(String subscriptionName) {
            this.subscriptionName = subscriptionName;
            return this;
        }

        public DefaultCatConsumer build() {
            checkValidation();
            return new DefaultCatConsumer(tenantId, topicDetail.getCompleteTopicName(), client, subscriptionName,
                    partitionSelector, handleResponseExecutor, handleRequestExecutor,
                    grpcConnectManager, topicZkInfo,
                    consumerId, batchPolicy);
        }

        public void checkValidation() {
            if (StringUtil.isEmpty(tenantId)) {
                throw new IllegalArgumentException("tenantId can not be empty");
            }
            if (client == null) {
                throw new IllegalArgumentException("client can not be null");
            }
            if (topicDetail == null) {
                throw new IllegalArgumentException("topic can not be null");
            }
            if (StringUtil.isEmpty(subscriptionName)) {
                subscriptionName = "default-subscription";
            }
            if (handleResponseExecutor == null) {
                handleResponseExecutor = ThreadPoolMonitor.createAndMonitor(1, 1, 5,
                        TimeUnit.SECONDS, "consumerResponseExecutor", 10);
            }
            if (handleRequestExecutor == null) {
                handleRequestExecutor = ThreadPoolMonitor.createAndMonitor(1, 1, 5,
                        TimeUnit.SECONDS, "consumerRequestExecutor", 10);
            }
            if (grpcConnectManager == null) {
                throw new IllegalArgumentException("grpcConnectManager can not be null");
            }
            if (topicZkInfo == null) {
                throw new IllegalArgumentException("topicZkInfo can not be null");
            }
        }

    }

}
