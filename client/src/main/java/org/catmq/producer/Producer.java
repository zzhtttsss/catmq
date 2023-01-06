package org.catmq.producer;

import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import lombok.extern.slf4j.Slf4j;
import org.catmq.protocol.service.BrokerServiceGrpc;
import org.catmq.protocol.service.SendMessage2BrokerRequest;
import org.catmq.protocol.service.SendMessage2BrokerResponse;
import org.catmq.util.StringUtil;

@Slf4j
public class Producer {

    private final BrokerServiceGrpc.BrokerServiceBlockingStub blockingStub;

    public void sendMessage2Broker(String topic, String message) {
        log.info(StringUtil.concatString("Will try to send message to broker, topic: ", topic, ", message: ", message));
        SendMessage2BrokerRequest request = SendMessage2BrokerRequest.newBuilder()
                .setMessage(message).build();
        SendMessage2BrokerResponse response;
        try {
            response = blockingStub.sendMessage2Broker(request);
        } catch (StatusRuntimeException e) {
            log.warn("RPC failed: {}", e.getStatus());
            return;
        }
        log.info("ack: " + response.getAck() + " response: " + response.getRes() + " status msg: " + response.getStatus().getMessage() + " status code: " + response.getStatus().getCode().getNumber());
    }

    public Producer(Channel channel) {
        // 'channel' here is a Channel, not a ManagedChannel, so it is not this code's responsibility to
        // shut it down.
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("action", Metadata.ASCII_STRING_MARSHALLER), "sendMessage2Broker");
        Channel headChannel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
        // Passing Channels to code makes code easier to test and makes it easier to reuse Channels.
        blockingStub = BrokerServiceGrpc.newBlockingStub(headChannel);
    }
}
