package org.catmq.storage.chunk;

import io.netty.util.internal.PlatformDependent;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
public class ChunkStorage {

    public static final long MAX_CACHE_SIZE = (long) (0.25 * PlatformDependent.estimateMaxDirectMemory());

    @Getter
    private WriteCache writeCache4Append;

    @Getter
    private WriteCache writeCache4Flush;

    @Getter
    private ReadCache readCache;

//    private CopyOnWriteArrayList<>

    public ChunkStorage() {
        this.writeCache4Append = new WriteCache(MAX_CACHE_SIZE);
        this.writeCache4Flush = new WriteCache(MAX_CACHE_SIZE);
        this.readCache = new ReadCache();
    }



}
