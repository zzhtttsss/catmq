package org.catmq.client;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.catmq.client.common.*;
import org.catmq.entity.GrpcConnectManager;
import org.catmq.entity.TopicDetail;
import org.catmq.protocol.definition.Code;
import org.catmq.protocol.definition.OriginMessage;
import org.catmq.protocol.definition.ProcessMode;
import org.catmq.protocol.service.BrokerServiceGrpc;
import org.catmq.protocol.service.SendMessage2BrokerRequest;
import org.catmq.protocol.service.SendMessage2BrokerResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;


@Slf4j
public class DefaultCatProducer extends ClientConfig {

    @Getter
    private final String tenantId;
    @Getter
    private final TopicDetail topicDetail;
    @Getter
    private long producerId;
    @Getter
    private final CuratorFramework client;
    private PartitionSelector partitionSelector;

    private final ThreadPoolExecutor handleResponseExecutor;

    private final ThreadPoolExecutor handleRequestExecutor;

    private final GrpcConnectManager grpcConnectManager;


    private DefaultCatProducer(String tenantId, String topic, CuratorFramework client,
                               PartitionSelector partitionSelector, ThreadPoolExecutor handleResponseExecutor,
                               ThreadPoolExecutor handleRequestExecutor, GrpcConnectManager grpcConnectManager) {
        this.tenantId = tenantId;
        this.topicDetail = TopicDetail.get(topic);
        log.warn("producer topic detail: {}", topicDetail.getCompleteTopicName());
        this.client = client;
        this.partitionSelector = partitionSelector;
        this.producerId = 1111L;
        this.handleRequestExecutor = handleRequestExecutor;
        this.handleResponseExecutor = handleResponseExecutor;
        this.grpcConnectManager = grpcConnectManager;

    }


    public void sendMessage(MessageEntry messageEntry, long timeout) {
        doSend(messageEntry, ProcessMode.SYNC, null, timeout);
    }


    public void sendMessage(Collection<? extends MessageEntry> messages, long timeout) {
        BatchMessageEntry batchMessageEntry = BatchMessageEntry.generateFromList(messages);
        sendMessage(batchMessageEntry, timeout);
    }

    public void asyncSendMessage(final MessageEntry messageEntry, final SendCallback sendCallback, final long timeout) {
        final long beginStartTime = System.currentTimeMillis();
        Runnable runnable = () -> {
            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeout > costTime) {
                doSend(messageEntry, ProcessMode.ASYNC, sendCallback, timeout - costTime);
            } else {
                sendCallback.onException(
                        new Exception("time out!"));
            }
        };
        asyncExecuteMessageSend(runnable);
    }

    private void asyncExecuteMessageSend(Runnable runnable) {
        try {
            handleRequestExecutor.submit(runnable);
        } catch (RejectedExecutionException e) {
            runnable.run();
        }
    }

    public void sendMessage(Collection<? extends MessageEntry> messages, final SendCallback sendCallback, final long timeout) {
        BatchMessageEntry batchMessageEntry = BatchMessageEntry.generateFromList(messages);
        asyncSendMessage(batchMessageEntry, sendCallback, timeout);
    }

    private void doSend(MessageEntry messageEntry, ProcessMode processMode, SendCallback sendCallback, long timeout) {
        ManagedChannel channel = grpcConnectManager.get("127.0.0.1:5432");
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("action", Metadata.ASCII_STRING_MARSHALLER), "sendMessage");
        Channel headChannel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
        BrokerServiceGrpc.BrokerServiceFutureStub futureStub = BrokerServiceGrpc.newFutureStub(headChannel);
        List<OriginMessage> messages = new ArrayList<>();
        if (messageEntry instanceof BatchMessageEntry batchMessageEntry) {
            for (MessageEntry me : batchMessageEntry) {
                OriginMessage message = OriginMessage.newBuilder()
                        .setBody(ByteString.copyFrom(me.getBody()))
                        .build();
                messages.add(message);
            }
        } else {
            OriginMessage message = OriginMessage.newBuilder()
                    .setBody(ByteString.copyFrom(messageEntry.getBody()))
                    .build();
            messages.add(message);
        }

        SendMessage2BrokerRequest request = SendMessage2BrokerRequest.newBuilder()
                .addAllMessage(messages)
                .setTopic(topicDetail.getCompleteTopicName() + "#0")
                .setProducerId(this.producerId)
                .build();
        ListenableFuture<SendMessage2BrokerResponse> responseFuture = futureStub.sendMessage2Broker(request);


        switch (processMode) {
            case SYNC -> {
                try {
                    SendMessage2BrokerResponse response = responseFuture.get();
                    if (response.getStatus().getCode() != Code.OK) {
                        log.error("fail..., message: {}", response.getStatus().getMessage());
                    }

                } catch (ExecutionException | InterruptedException e) {
                    log.error("Fail to get response.", e);
                }
            }
            case ASYNC -> Futures.addCallback(responseFuture, new FutureCallback<>() {
                @Override
                public void onSuccess(SendMessage2BrokerResponse result) {
                    sendCallback.onSuccess(new SendResult());
                }

                @Override
                public void onFailure(Throwable t) {
                    sendCallback.onException(t);
                }
            }, MoreExecutors.directExecutor());
        }
    }


    protected static DefaultCatProducerBuilder builder(String tenantId, CuratorFramework client,
                                                       ThreadPoolExecutor handleRequestExecutor,
                                                       ThreadPoolExecutor handleResponseExecutor,
                                                       GrpcConnectManager grpcConnectManager) {
        return new DefaultCatProducerBuilder(tenantId, client, handleRequestExecutor, handleResponseExecutor, grpcConnectManager);
    }

    public static class DefaultCatProducerBuilder {
        private final String tenantId;

        private String topic;

        private CuratorFramework client;

        private PartitionSelector partitionSelector;

        private final ThreadPoolExecutor handleResponseExecutor;

        private final ThreadPoolExecutor handleRequestExecutor;

        private final GrpcConnectManager grpcConnectManager;

        protected DefaultCatProducerBuilder(String tenantId, CuratorFramework client, ThreadPoolExecutor handleRequestExecutor,
                                            ThreadPoolExecutor handleResponseExecutor, GrpcConnectManager grpcConnectManager) {
            this.tenantId = tenantId;
            this.client = client;
            this.handleRequestExecutor = handleRequestExecutor;
            this.handleResponseExecutor = handleResponseExecutor;
            this.grpcConnectManager = grpcConnectManager;
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
            return new DefaultCatProducer(tenantId, topic, client, partitionSelector,
                    handleRequestExecutor, handleResponseExecutor, grpcConnectManager);
        }
    }


}
