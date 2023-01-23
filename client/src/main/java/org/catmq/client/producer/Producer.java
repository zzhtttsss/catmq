package org.catmq.client.producer;

import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import lombok.extern.slf4j.Slf4j;
import org.catmq.common.TopicDetail;
import org.catmq.protocol.service.*;

@Slf4j
public class Producer {

    private final BrokerServiceGrpc.BrokerServiceBlockingStub blockingStub;

    private final long producerId;

    private final ProducerConfig config;

    private final ProducerZooKeeper producerZooKeeper;


    public void sendMessage2Broker(String topic, String message) {
//        log.info("Will try to send message to broker, topic: {} , message: {}", topic, message);
//        if (!TopicDetail.containsKey(topic)) {
//            log.info("Topic [{}] not exists. Ready to create.", topic);
//            createTopic(topic);
//        }
//        TopicDetail topicDetail = TopicDetail.get(topic);
//        SendMessage2BrokerRequest request = SendMessage2BrokerRequest.newBuilder()
//                .setMessage(ByteString.copyFrom(message.getBytes()))
//                .setTopic(topicDetail.getCompleteTopicName())
//                .build();
//        SendMessage2BrokerResponse response;
//        try {
//            response = blockingStub.sendMessage2Broker(request);
//        } catch (StatusRuntimeException e) {
//            log.warn("RPC failed: {}", e.getMessage());
//            return;
//        }
//        log.info("ack: {} \nresponse: {} \nstatus msg: {} \nstatus code: {}",
//                response.getAck(), response.getRes(), response.getStatus().getMessage(),
//                response.getStatus().getCode().getNumber());
    }

    public void createTopic(String topic) {
        TopicDetail topicDetail = TopicDetail.get(topic);
        CreateTopicRequest request = CreateTopicRequest
                .newBuilder()
                .setTopic(topicDetail.getCompleteTopicName())
                .setProducerId(producerId)
                .build();
        CreateTopicResponse response = null;
        try {
            response = blockingStub.createTopic(request);
        } catch (StatusRuntimeException e) {
            log.warn("RPC failed: {}", e.getMessage());
            return;
        }
        log.info("{}", response.toString());
    }

    public void close() {
        producerZooKeeper.close();
    }

    private void init() {
        producerZooKeeper.register2Zk();
    }

    public Producer(Channel channel, long producerId) {
        this.producerId = producerId;
        // 'channel' here is a Channel, not a ManagedChannel, so it is not this code's responsibility to
        // shut it down.
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("action", Metadata.ASCII_STRING_MARSHALLER), "createTopic");
        Channel headChannel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
        // Passing Channels to code makes code easier to test and makes it easier to reuse Channels.
        blockingStub = BrokerServiceGrpc.newBlockingStub(headChannel);
        config = ProducerConfig.ProducerConfigEnum.INSTANCE.getInstance();
        producerZooKeeper = new ProducerZooKeeper(config);
        init();
    }


}
