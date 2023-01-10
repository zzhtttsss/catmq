package org.catmq.storage.chunk;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

@Slf4j
public class PartitionSegment {
    @Getter
    public long segmentId;

}
