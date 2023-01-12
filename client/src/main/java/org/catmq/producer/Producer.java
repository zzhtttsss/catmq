package org.catmq.producer;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.topic.TopicName;
import org.catmq.producer.zk.ProducerZooKeeper;
import org.catmq.protocol.service.*;

@Slf4j
public class Producer {

    private final BrokerServiceGrpc.BrokerServiceBlockingStub blockingStub;

    private final ProducerConfig config;

    private final ProducerZooKeeper producerZooKeeper;


    public void sendMessage2Broker(String topic, String message) {
        log.info("Will try to send message to broker, topic: {} , message: {}", topic, message);
        TopicName topicName = TopicName.get(topic);
        if (!producerZooKeeper.checkTopicExists(topicName)) {
            log.info("Topic [{}] not exists. Ready to create.", topic);
            createTopic(topicName);
        }
        SendMessage2BrokerRequest request = SendMessage2BrokerRequest.newBuilder()
                .setMessage(message)
                .setTopic(topicName.getCompleteTopicName())
                .build();
        SendMessage2BrokerResponse response;
        try {
            response = blockingStub.sendMessage2Broker(request);
        } catch (StatusRuntimeException e) {
            log.warn("RPC failed: {}", e.getMessage());
            return;
        }
        log.info("ack: " + response.getAck() + " response: " + response.getRes() + " status msg: " + response.getStatus().getMessage() + " status code: " + response.getStatus().getCode().getNumber());
    }

    public void createTopic(TopicName topic) {
        CreateTopicRequest request = CreateTopicRequest
                .newBuilder()
                .setTopic(topic.getCompleteTopicName())
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

    /**
     * Check topic whether exists in broker connected.
     * If not, create it in broker.
     *
     * @param topic topic name
     * @return true if topic exists in broker, otherwise false
     */
    @VisibleForTesting
    private boolean checkTopicExists(String topic) {
        return true;
    }

    private void init() {
        producerZooKeeper.register2Zk();
    }

    public Producer(Channel channel) {
        // 'channel' here is a Channel, not a ManagedChannel, so it is not this code's responsibility to
        // shut it down.
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("action", Metadata.ASCII_STRING_MARSHALLER), "sendMessage2Broker");
        Channel headChannel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
        // Passing Channels to code makes code easier to test and makes it easier to reuse Channels.
        blockingStub = BrokerServiceGrpc.newBlockingStub(headChannel);
        config = ProducerConfig.ProducerConfigEnum.INSTANCE.getInstance();
        producerZooKeeper = new ProducerZooKeeper(config);
        init();
    }


}
