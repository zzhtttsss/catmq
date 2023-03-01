package org.catmq.client.consumer;

import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import lombok.extern.slf4j.Slf4j;
import org.catmq.protocol.service.BrokerServiceGrpc;
import org.catmq.protocol.service.GetMessageFromBrokerRequest;
import org.catmq.protocol.service.GetMessageFromBrokerResponse;

@Slf4j
public class Consumer {

    private final BrokerServiceGrpc.BrokerServiceBlockingStub blockingStub;

    private final long consumerId;

    private final ConsumerConfig config;

    private final ConsumerZooKeeper consumerZooKeeper;

    public void getMessageFromBroker(String topic) {
        log.info("Will try to get message from broker, topic: {}", topic);
        GetMessageFromBrokerRequest request = GetMessageFromBrokerRequest.newBuilder()
                .setTopic(topic)
                .setConsumerId(consumerId)
                .build();
        GetMessageFromBrokerResponse response;
        try {
            response = blockingStub.getMessageFromBroker(request);
        } catch (Exception e) {
            log.warn("RPC failed: {}", e.getMessage());
            return;
        }
        log.info("ack: {} \nresponse: {} \nstatus msg: {} \nstatus code: {} \nmessage: {}",
                response.getAck(), response.getRes(), response.getStatus().getMessage(),
                response.getStatus().getCode().getNumber(), response.getMessage(0));
    }

    public void close() {
        consumerZooKeeper.close();
    }

    private void init() {
        consumerZooKeeper.register2Zk();
        log.info("Consumer [{}] init success.", consumerId);
    }

    public Consumer(Channel channel, long consumerId) {
        this.consumerId = consumerId;
        blockingStub = BrokerServiceGrpc.newBlockingStub(ClientInterceptors.intercept(channel));
        config = ConsumerConfig.ConsumerConfigEnum.INSTANCE.getInstance();
        consumerZooKeeper = new ConsumerZooKeeper(config);
        init();
    }
}
