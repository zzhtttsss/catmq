package org.catmq.entity;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Manager of grpc connection whose key is broker path like /address/broker/127.0.0.1:5432
 */
@Slf4j
public class GrpcConnectManager {
    @Getter
    private final LoadingCache<String, ManagedChannel> cache;

    public GrpcConnectManager(int maxSize) {
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
                });
        ;
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

