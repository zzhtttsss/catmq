package org.catmq.storage.segment;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.common.FileChannelWrapper;
import org.catmq.common.MessageEntry;
import org.catmq.common.MessageEntryBatch;
import org.catmq.constant.CommonConstant;
import org.catmq.constant.FileConstant;
import org.catmq.util.Concat2String;
import org.catmq.util.StringUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.catmq.entity.StorerConfig.STORER_CONFIG;

@Slf4j
@Getter
public class SegmentFileManager {
    private final String directory;
    private final CopyOnWriteArrayList<Long> paths;

    public FileChannelWrapper getOrCreateSegmentFileByOffset(long offset, boolean isCreated) throws IOException {
        String fileName = buildSegmentFilePath(offset);
        File file = new File(fileName);
        if (!file.exists() && isCreated) {
            paths.add(offset);
            file.createNewFile();
        }
        return new FileChannelWrapper(file, "rw");
    }


    public MessageEntryBatch getSegmentBatchByOffset(long offset) {
        Long fileOffset = getSegmentFileOffsetByOffset(offset);
        // global offset - file offset = offset in file
        long segmentOffset = offset - fileOffset;
        MessageEntryBatch batch = new MessageEntryBatch();
        try (FileChannelWrapper wrapper = getOrCreateSegmentFileByOffset(fileOffset, false)) {
            FileChannel fc = wrapper.getFileChannel();
            var mapped = fc.map(FileChannel.MapMode.READ_ONLY, segmentOffset, 4 * FileConstant.KB);
            List<MessageEntry> entries = readFromByteBuf(mapped);
            batch.putAll(entries);
            return batch;
        } catch (IOException e) {
            log.error("Get segment batch by offset error, offset: {}", offset, e);
        }
        return batch;
    }

    public List<MessageEntry> readFromByteBuf(ByteBuffer buf) {
        List<MessageEntry> entries = new ArrayList<>();
        long firstSegmentId = -1;
        boolean isFirst = true;
        while (buf.position() + CommonConstant.BYTES_LENGTH_OF_INT < buf.limit()) {
            int length = buf.getInt();
            // 1. Remaining bytes is not enough to read a message, break.
            if (buf.position() + length > buf.limit()) {
                // It should not happen.
                break;
            }
            long segmentId = buf.getLong();
            if (isFirst) {
                firstSegmentId = segmentId;
                isFirst = false;
            }
            // 2. Segment id is not continuous, break.
            if (firstSegmentId != segmentId) {
                break;
            }
            long entryId = buf.getLong();
            byte[] bytes = new byte[length - 16];
            buf.get(bytes);
            entries.add(new MessageEntry(segmentId, entryId, bytes));
            // 3. Read at most 500 messages once.
            if (entries.size() >= 500) {
                break;
            }
        }
        return entries;
    }

    public Long getSegmentFileOffsetByOffset(long offset) {
        if (paths.isEmpty()) {
            throw new RuntimeException("no segment file");
        }
        int index = Collections.binarySearch(paths, offset);
        if (index < 0) {
            // If index < 0, it means that -(insert position)-1.
            index = -index - 2;
        }
        return paths.get(index);
    }

    private String buildSegmentFilePath(long offset) {
        return Concat2String.builder()
                .concat(directory)
                .concat(FileConstant.LEFT_SLASH)
                .concat(StringUtil.offset2FileName(offset))
                .build();
    }

    public SegmentFileManager() {
        this.directory = STORER_CONFIG.getSegmentStoragePath();
        this.paths = new CopyOnWriteArrayList<>();
        File dir = new File(this.directory);
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                paths.add(Long.parseLong(file.getName()));
            }
        }
    }

    public enum SegmentFileServiceEnum {
        INSTANCE;
        private final SegmentFileManager segmentFileManager;

        SegmentFileServiceEnum() {
            this.segmentFileManager = new SegmentFileManager();
        }

        public SegmentFileManager getInstance() {
            return segmentFileManager;
        }
    }
}
