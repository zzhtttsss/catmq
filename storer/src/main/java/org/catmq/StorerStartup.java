package org.catmq;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.protobuf.services.ChannelzService;
import io.grpc.protobuf.services.ProtoReflectionService;
import lombok.extern.slf4j.Slf4j;
import org.catmq.grpc.ContextInterceptor;
import org.catmq.storer.StorerServer;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.catmq.storer.Storer.STORER;

@Slf4j
public class StorerStartup {

    private static Server server;

    public static void start() throws IOException {
        STORER.init();
        int port = 5432;
        server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .addService(new StorerServer())
                .addService(ChannelzService.newInstance(100))
                .addService(ProtoReflectionService.newInstance())
                .intercept(new ContextInterceptor())
                .build()
                .start();

        log.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    StorerStartup.stop();
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

    public static void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        StorerStartup.start();
        StorerStartup.blockUntilShutdown();
    }
}
