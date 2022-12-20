package org.catmq;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import org.catmq.context.Context;
import org.catmq.executer.Executer;
import org.catmq.protocol.produce.ProduceServiceGrpc;
import org.catmq.protocol.produce.SendMessage2BrokerReply;
import org.catmq.protocol.produce.SendMessage2BrokerRequest;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class BrokerServer {
    private static final Logger logger = Logger.getLogger(BrokerServer.class.getName());

    private static Server server;

    public static void start() throws IOException {
        /* The port on which the server should run */
        int port = 5432;
        server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .addService(new ProduceServiceImpl())
                .build()
                .start();

        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    BrokerServer.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    public static void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public static void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    static class ProduceServiceImpl extends ProduceServiceGrpc.ProduceServiceImplBase {
        @Override
        public void sendMessage2Broker(SendMessage2BrokerRequest req, StreamObserver<SendMessage2BrokerReply> responseObserver) {
            Context ctx = new Context("a", "res: ");
            Executer executer = Executer.getExecuterByContext(ctx);
            ctx = executer.execute(ctx);
            SendMessage2BrokerReply reply = SendMessage2BrokerReply.newBuilder().setAck(true).setRes(ctx.testField).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }
}
