package org.catmq.common;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import lombok.NonNull;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class ConnectCache {

    private static final LoadingCache<String, ManagedChannel> CACHE = CacheBuilder
            .newBuilder()
            .maximumSize(100)
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

    public static ManagedChannel get(String address) {
        return CACHE.getUnchecked(address);
    }

    public static void remove(String address) {
        CACHE.invalidate(address);
    }

    public static boolean containsKey(String address) {
        return Optional.of(address).map(CACHE::getIfPresent).isPresent();
    }


}
