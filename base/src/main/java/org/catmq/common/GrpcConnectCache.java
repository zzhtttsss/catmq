package org.catmq.common;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import lombok.Getter;
import lombok.NonNull;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class GrpcConnectCache {
    @Getter
    private final LoadingCache<String, ManagedChannel> cache;

    public GrpcConnectCache(int maxSize) {
        this.cache = CacheBuilder
                .newBuilder()
                .maximumSize(maxSize)
                .expireAfterAccess(30, TimeUnit.MINUTES)
                .build(new CacheLoader<>() {
                    @Override
                    public @NonNull ManagedChannel load(@NonNull String address) {
                        return Grpc.newChannelBuilder(address, InsecureChannelCredentials.create())
                                .keepAliveTime(30, TimeUnit.SECONDS)
                                .keepAliveTimeout(10, TimeUnit.SECONDS)
                                .build();
                    }
                });;
    }

    public ManagedChannel get(String key) {
        return cache.getUnchecked(key);
    }

    public void remove(String key) {
        cache.invalidate(key);
    }

    public boolean containsKey(String key) {
        return Optional.of(key).map(cache::getIfPresent).isPresent();
    }

}

