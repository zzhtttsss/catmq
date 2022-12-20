package org.catmq;

import io.grpc.*;
import org.catmq.protocol.produce.ProduceServiceGrpc;
import org.catmq.protocol.produce.SendMessage2BrokerReply;
import org.catmq.protocol.produce.SendMessage2BrokerRequest;
import org.catmq.util.StringUtil;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Producer {

    private static final Logger logger = Logger.getLogger(Producer.class.getName());

    private final ProduceServiceGrpc.ProduceServiceBlockingStub blockingStub;
    /** Construct client for accessing HelloWorld server using the existing channel. */
    public Producer(Channel channel) {
        // 'channel' here is a Channel, not a ManagedChannel, so it is not this code's responsibility to
        // shut it down.

        // Passing Channels to code makes code easier to test and makes it easier to reuse Channels.
        blockingStub = ProduceServiceGrpc.newBlockingStub(channel);
    }

    public void sendMessage2Broker(String topic, String message) {
        logger.info(StringUtil.concatString("Will try to send message to broker, topic: ", topic, ", message: ", message));
        SendMessage2BrokerRequest request = SendMessage2BrokerRequest.newBuilder()
                .setTopic(topic).setMessage(message).build();
        SendMessage2BrokerReply response;
        try {
            response = blockingStub.sendMessage2Broker(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("ack: " + response.getAck() + " response: " + response.getRes());
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
