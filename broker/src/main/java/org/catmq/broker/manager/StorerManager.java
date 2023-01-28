package org.catmq.broker.manager;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import lombok.extern.slf4j.Slf4j;
import org.catmq.entity.TopicMode;
import org.catmq.protocol.definition.NumberedMessage;
import org.catmq.protocol.service.*;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.catmq.broker.Broker.BROKER;
import static org.catmq.thread.ListenableFutureAdapter.toCompletable;

@Slf4j
public class StorerManager {

    private StorerManager() {

    }

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

    public CompletableFuture<List<SendMessage2StorerResponse>> sendMessage2Storer(List<NumberedMessage> messages,
                                                                                  TopicMode topicMode, String[] storerAddresses) {
        List<ListenableFuture<SendMessage2StorerResponse>> listenableFutures = new ArrayList<>();
        for (String address : storerAddresses) {
            listenableFutures.add(doSendMessage2Storer(messages, topicMode, address));
        }
        ListenableFuture<List<SendMessage2StorerResponse>> listenableFuture = Futures.allAsList(listenableFutures);
        return toCompletable(listenableFuture);
    }

    private ListenableFuture<SendMessage2StorerResponse> doSendMessage2Storer(List<NumberedMessage> messages,
                                                                              TopicMode topicMode, String storerAddress) {
        ManagedChannel channel = BROKER.getGrpcConnectManager().get(storerAddress);
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("action", Metadata.ASCII_STRING_MARSHALLER), "sendMessage");
        Channel headChannel = ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(metadata));
        StorerServiceGrpc.StorerServiceFutureStub futureStub = StorerServiceGrpc.newFutureStub(headChannel);
        SendMessage2StorerRequest request = SendMessage2StorerRequest.newBuilder()
                .addAllMessage(messages)
                .setMode(topicMode.getName())
                .build();
        return futureStub.sendMessage2Storer(request);
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
