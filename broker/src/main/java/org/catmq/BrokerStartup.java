package org.catmq;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.protobuf.services.ChannelzService;
import io.grpc.protobuf.services.ProtoReflectionService;
import lombok.extern.slf4j.Slf4j;
import org.catmq.context.ContextInterceptor;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class BrokerStartup {


    private static Server server;

    public static void start() throws IOException {
        /* The port on which the server should run */
        int port = 5432;
        server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .addService(new BrokerServer())
                .addService(ChannelzService.newInstance(100))
                .addService(ProtoReflectionService.newInstance())
                .intercept(new ContextInterceptor())
                .build()
                .start();

        log.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    BrokerStartup.stop();
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


    public static void main(String[] args) throws IOException, InterruptedException {
        BrokerStartup.start();
        BrokerStartup.blockUntilShutdown();
    }
}
