package org.catmq;

import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import org.catmq.protocol.service.BrokerServiceGrpc;
import org.catmq.protocol.service.SendMessage2BrokerRequest;
import org.catmq.protocol.service.SendMessage2BrokerResponse;
import org.catmq.util.StringUtil;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Producer {

    private static final Logger logger = Logger.getLogger(Producer.class.getName());

    private final BrokerServiceGrpc.BrokerServiceBlockingStub blockingStub;
    /** Construct client for accessing HelloWorld server using the existing channel. */
    public Producer(Channel channel) {
        // 'channel' here is a Channel, not a ManagedChannel, so it is not this code's responsibility to
        // shut it down.
        Metadata metadata = new Metadata();
        metadata.put( Metadata.Key.of("action", Metadata.ASCII_STRING_MARSHALLER), "sendMessage2Broker");
        Channel headChannel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
        // Passing Channels to code makes code easier to test and makes it easier to reuse Channels.
        blockingStub = BrokerServiceGrpc.newBlockingStub(headChannel);
    }

    public void sendMessage2Broker(String topic, String message) {
        logger.info(StringUtil.concatString("Will try to send message to broker, topic: ", topic, ", message: ", message));
        SendMessage2BrokerRequest request = SendMessage2BrokerRequest.newBuilder()
                .setMessage(message).build();
        SendMessage2BrokerResponse response;
        try {
            response = blockingStub.sendMessage2Broker(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("ack: " + response.getAck() + " response: " + response.getRes() + " status msg: " + response.getStatus().getMessage() + " status code: " + response.getStatus().getCode().getNumber());
    }

    public static void main(String[] args) throws Exception {
        // Access a service running on the local machine on port 50051
        String target = "localhost:5432";

        // Create a communication channel to the server, known as a Channel. Channels are thread-safe
        // and reusable. It is common to create channels at the beginning of your application and reuse
        // them until the application shuts down.
        //
        // For the example we use plaintext insecure credentials to avoid needing TLS certificates. To
        // use TLS, use TlsChannelCredentials instead.
        ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
        try {
            Producer client = new Producer(channel);
            client.sendMessage2Broker("t1", "first message.");
        } finally {
            // ManagedChannels use resources like threads and TCP connections. To prevent leaking these
            // resources the channel should be shut down when it will no longer be used. If it may be used
            // again leave it running.
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
