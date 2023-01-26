package org.catmq;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.protobuf.services.ChannelzService;
import io.grpc.protobuf.services.ProtoReflectionService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.catmq.grpc.ContextInterceptor;
import org.catmq.storer.StorerServer;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.catmq.storer.Storer.STORER;
import static org.catmq.entity.StorerConfig.STORER_CONFIG;

/**
 * Start the storer service.
 */
@Slf4j
public class StorerStartup {
    private static Server server;
    private static final int WAIT_TIMEOUT = 30;

    public static void start() throws IOException {
        directoryInit();
        int port = STORER_CONFIG.getStorerPort();
        STORER.init();
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

    private static void directoryInit() {
        boolean success = true;
        File segmentDir = new File(STORER_CONFIG.getSegmentStoragePath());
        if (!segmentDir.exists()) {
            success = segmentDir.mkdir();
        } else {
            try {
                FileUtils.cleanDirectory(segmentDir);
            } catch (IOException e) {
                log.error("clean segment directory failed.", e);
            }
        }
        File messageLogDir = new File(STORER_CONFIG.getMessageLogStoragePath());
        if (!messageLogDir.exists()) {
            success &= messageLogDir.mkdir();
        } else {
            try {
                FileUtils.cleanDirectory(messageLogDir);
            } catch (IOException e) {
                log.error("clean message log directory failed.", e);
            }
        }
        File indexDir = new File(STORER_CONFIG.getSegmentIndexStoragePath());
        if (!indexDir.exists()) {
            success &= indexDir.mkdir();
        } else {
            try {
                FileUtils.cleanDirectory(indexDir);
            } catch (IOException e) {
                log.error("clean index directory failed.", e);
            }
        }
        if (success) {
            log.info("Create directories successfully");
        } else {
            log.error("Fail to create directories");
            System.exit(-1);
        }

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
