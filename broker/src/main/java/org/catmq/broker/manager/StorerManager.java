package org.catmq.broker.manager;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.*;
import io.grpc.stub.MetadataUtils;
import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.common.NumberedMessageBatch;
import org.catmq.entity.TopicMode;
import org.catmq.protocol.service.*;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.catmq.broker.Broker.BROKER;
import static org.catmq.thread.ListenableFutureAdapter.toCompletable;

@Slf4j
public class StorerManager {

    private StorerManager() {

    }

    @Deprecated
    public void createSegment(long segmentId) {
        try {
            String[] storerZkPaths = BROKER.getBrokerZkManager().selectStorer(1).orElseThrow();
            List<ListenableFuture<CreateSegmentResponse>> listenableFutures = new ArrayList<>();
            for (String path : storerZkPaths) {
                listenableFutures.add(doCreateSegment(path, segmentId));
            }
            ListenableFuture<List<CreateSegmentResponse>> listenableFuture = Futures.allAsList(listenableFutures);
            listenableFuture.get();
        } catch (InterruptedException | ExecutionException | NoSuchElementException e) {
            throw new RuntimeException(e);
        }
    }

    @Deprecated
    private ListenableFuture<CreateSegmentResponse> doCreateSegment(String storerZkPath, long segmentId) {
        ManagedChannel channel = BROKER.getGrpcConnectManager().get(storerZkPath);
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("action", Metadata.ASCII_STRING_MARSHALLER), "createPartition");
        Channel headChannel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
        StorerServiceGrpc.StorerServiceFutureStub futureStub = StorerServiceGrpc.newFutureStub(headChannel);

        CreateSegmentRequest request = CreateSegmentRequest.newBuilder()
                .setSegmentId(segmentId)
                .build();
        return futureStub.createSegment(request);
    }

    public CompletableFuture<List<SendMessage2StorerResponse>> sendMessage2Storer(NumberedMessageBatch messages,
                                                                                  TopicMode topicMode, String[] storerAddresses) {
        List<ListenableFuture<SendMessage2StorerResponse>> listenableFutures = new ArrayList<>();
        // send message to each storer.
        for (String address : storerAddresses) {
            listenableFutures.add(doSendMessage2Storer(messages, topicMode, address));
        }
        ListenableFuture<List<SendMessage2StorerResponse>> listenableFuture = Futures.allAsList(listenableFutures);
        return toCompletable(listenableFuture);
    }

    public CompletableFuture<GetMessageFromStorerResponse> getMessageFromStorer(long segmentId,
                                                                                long entryId,
                                                                                String[] storerAddresses) {
        CompletableFuture<GetMessageFromStorerResponse> res = null;
        for (String address : storerAddresses) {
            try {
                res = doGetMessageFromStorer(segmentId, entryId, address);
                break;
            } catch (StatusRuntimeException e) {
                log.warn("Get message from storer[{}] error.", address, e);
            }
        }
        return res;

    }

    private ListenableFuture<SendMessage2StorerResponse> doSendMessage2Storer(NumberedMessageBatch messages,
                                                                              TopicMode topicMode,
                                                                              String storerAddress) {
        ManagedChannel channel = BROKER.getGrpcConnectManager().get(storerAddress);
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("action", Metadata.ASCII_STRING_MARSHALLER), "sendMessage");
        Channel headChannel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
        StorerServiceGrpc.StorerServiceFutureStub futureStub = StorerServiceGrpc.newFutureStub(headChannel);
        SendMessage2StorerRequest request = SendMessage2StorerRequest.newBuilder()
                .addAllMessage(messages.getBatch())
                .setMode(topicMode.getName())
                .build();
        return futureStub.sendMessage2Storer(request);
    }

    private CompletableFuture<GetMessageFromStorerResponse> doGetMessageFromStorer(long segmentId,
                                                                                   long entryId,
                                                                                   String storerAddress) throws StatusRuntimeException {
        ManagedChannel channel = BROKER.getGrpcConnectManager().get(storerAddress);
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("action", Metadata.ASCII_STRING_MARSHALLER), "getMessage");
        Channel headChannel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
        StorerServiceGrpc.StorerServiceFutureStub futureStub = StorerServiceGrpc.newFutureStub(headChannel);
        GetMessageFromStorerRequest request = GetMessageFromStorerRequest.newBuilder()
                .setSegmentId(segmentId)
                .setEntryId(entryId)
                .build();
        return toCompletable(futureStub.withDeadlineAfter(3, TimeUnit.MILLISECONDS).getMessageFromStorer(request));

    }


    public enum StorerManagerEnum {
        INSTANCE;
        private final StorerManager storerManager;

        public StorerManager getInstance() {
            return storerManager;
        }

        StorerManagerEnum() {
            storerManager = new StorerManager();
        }
    }

}
