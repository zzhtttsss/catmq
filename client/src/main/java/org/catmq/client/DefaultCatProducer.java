package org.catmq.client;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.catmq.client.common.*;
import org.catmq.common.TopicDetail;
import org.catmq.protocol.definition.Message;
import org.catmq.protocol.definition.MessageType;
import org.catmq.protocol.service.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import static org.catmq.client.CatClient.GRPC_CONNECT_CACHE;

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

    private final ThreadPoolExecutor handleResultExecutor;

    private final ThreadPoolExecutor handleSendExecutor;

    private final ThreadPoolExecutor handleGrpcResponseExecutor;



    private DefaultCatProducer(String tenantId, String producerGroup, String topic, CuratorFramework client,
                               PartitionSelector partitionSelector, ThreadPoolExecutor handleResultExecutor,
                               ThreadPoolExecutor handleSendExecutor, ThreadPoolExecutor handleGrpcResponseExecutor) {
        this.tenantId = tenantId;
        this.producerGroup = producerGroup;
        this.topicDetail = TopicDetail.get(topic);
        this.client = client;
        this.partitionSelector = partitionSelector;
        this.topicDetail.setBrokerAddress("127.0.0.1:5432");
        this.producerId = 1111L;
        this.handleSendExecutor = handleSendExecutor;
        this.handleResultExecutor = handleResultExecutor;
        this.handleGrpcResponseExecutor = handleGrpcResponseExecutor;
    }


    public void sendMessage(MessageEntry messageEntry, long timeout) {
        doSend(messageEntry, ProcessMode.SYNC, null, timeout);
    }


    public void sendMessage(Collection<? extends MessageEntry> messages, long timeout) {
        BatchMessageEntry batchMessageEntry = BatchMessageEntry.generateFromList(messages);
        sendMessage(batchMessageEntry, timeout);
    }

    public void sendMessage(final MessageEntry messageEntry, final SendCallback sendCallback, final long timeout) {
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
            handleSendExecutor.submit(runnable);
        } catch (RejectedExecutionException e) {
            runnable.run();
        }
    }

    public void sendMessage(Collection<? extends MessageEntry> messages, final SendCallback sendCallback, final long timeout) {
        BatchMessageEntry batchMessageEntry = BatchMessageEntry.generateFromList(messages);
        sendMessage(batchMessageEntry, sendCallback, timeout);
    }

    private void doSend(MessageEntry messageEntry, ProcessMode processMode, SendCallback sendCallback, long timeout) {
        ManagedChannel channel = GRPC_CONNECT_CACHE.get(topicDetail.getBrokerAddress());
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("action", Metadata.ASCII_STRING_MARSHALLER), "sendMessage");
        Channel headChannel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
        BrokerServiceGrpc.BrokerServiceFutureStub futureStub = BrokerServiceGrpc.newFutureStub(headChannel);

        List<Message> messages = new ArrayList<>();
        if (messageEntry instanceof BatchMessageEntry batchMessageEntry) {
            for (MessageEntry me : batchMessageEntry) {
                Message message = Message.newBuilder()
                        .setTopic(topicDetail.getCompleteTopicName())
                        .setBody(ByteString.copyFrom(messageEntry.getBody()))
                        .setType(MessageType.NORMAL)
                        .build();
                messages.add(message);
            }
        }
        else {
            Message message = Message.newBuilder()
                    .setTopic(topicDetail.getCompleteTopicName())
                    .setBody(ByteString.copyFrom(messageEntry.getBody()))
                    .setType(MessageType.NORMAL)
                    .build();
            messages.add(message);
        }

        SendMessage2BrokerRequest request = SendMessage2BrokerRequest.newBuilder()
                .addAllMessage(messages)
                .setProducerId(this.producerId)
                .build();
        ListenableFuture<SendMessage2BrokerResponse> responseFuture = futureStub.sendMessage2Broker(request);

        switch (processMode) {
            case SYNC -> {
                try {
                    responseFuture.get();
                } catch (ExecutionException | InterruptedException e) {
                    log.error("Fail to get response.", e);
                }
            }
            case ASYNC -> Futures.addCallback(responseFuture, new FutureCallback<>() {
                @Override
                public void onSuccess(SendMessage2BrokerResponse result) {
                    log.info("success to send message");
                    sendCallback.onSuccess(new SendResult());
                }

                @Override
                public void onFailure(Throwable t) {
                    log.warn("fail to send message", t);
                    sendCallback.onException(t);
                }
            }, handleResultExecutor);
        }



    }


    protected static DefaultCatProducerBuilder builder(String tenantId, CuratorFramework client,
                                                       ThreadPoolExecutor handleRequestExecutor,
                                                       ThreadPoolExecutor handleResponseExecutor,
                                                       ThreadPoolExecutor handleGrpcResponseExecutor) {
        return new DefaultCatProducerBuilder(tenantId, client, handleRequestExecutor, handleResponseExecutor,
                handleGrpcResponseExecutor);
    }

    public static class DefaultCatProducerBuilder {
        private final String tenantId;
        private String producerGroup;

        private String topic;

        private CuratorFramework client;

        private PartitionSelector partitionSelector;

        private final ThreadPoolExecutor handleResultExecutor;

        private final ThreadPoolExecutor handleSendExecutor;

        private final ThreadPoolExecutor handleGrpcResponseExecutor;


        protected DefaultCatProducerBuilder(String tenantId, CuratorFramework client,
                                            ThreadPoolExecutor handleSendExecutor,
                                            ThreadPoolExecutor handleResultExecutor,
                                            ThreadPoolExecutor handleGrpcResponseExecutor) {
            this.tenantId = tenantId;
            this.client = client;
            this.handleSendExecutor = handleSendExecutor;
            this.handleResultExecutor = handleResultExecutor;
            this.handleGrpcResponseExecutor = handleGrpcResponseExecutor;
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
            return new DefaultCatProducer(tenantId, producerGroup, topic, client, partitionSelector,
                    handleSendExecutor, handleResultExecutor, handleGrpcResponseExecutor);
        }
    }


}
