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
import static org.catmq.storer.StorerConfig.STORER_CONFIG;

@Slf4j
public class StorerStartup {

    private static Server server;

    private static final int WAIT_TIMEOUT = 30;

    public static void start() throws IOException {
        STORER.init();
        int port = STORER_CONFIG.getStorerPort();
        server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .addService(new StorerServer())
                .addService(ChannelzService.newInstance(100))
                .addService(ProtoReflectionService.newInstance())
                .intercept(new ContextInterceptor())
                .build()
                .start();

        log.info("Server started, listening on {}.", port);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.error("*** shutting down gRPC server since JVM is shutting down.");
            try {
                stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
            log.error("*** server shut down.");
        }));
    }

    public static void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(WAIT_TIMEOUT, TimeUnit.SECONDS);
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
