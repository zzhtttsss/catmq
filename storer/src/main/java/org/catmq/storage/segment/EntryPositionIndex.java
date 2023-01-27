package org.catmq.storage.segment;

import lombok.extern.slf4j.Slf4j;
import org.catmq.storage.segment.KeyValueStorage.Batch;
import org.catmq.util.ByteUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map.Entry;

/**
 * Manage the index of all message.
 */
@Slf4j
public class EntryPositionIndex implements Closeable {

    private static final String SUB_PATH = "positions";

    private KeyValueStorage positionsDb;

    public EntryPositionIndex(KeyValueStorageFactory storageFactory, String basePath) {
        try {
            positionsDb = storageFactory.newKeyValueStorage(basePath, SUB_PATH,
                    KeyValueStorageFactory.DbConfigType.EntryPosition);
        } catch (IOException e) {
            positionsDb = null;
            log.error("Fail to build locationsDb.", e);
        }
    }

    @Override
    public void close() throws IOException {
        positionsDb.close();
    }

    public long getPosition(long segmentId, long entryId) throws IOException {
        byte[] key = ByteUtil.convLong2Bytes(segmentId, entryId);
        byte[] value = new byte[Long.BYTES];
        if (positionsDb.get(key, value) < 0) {
            return 0;
        }
        return ByteUtil.getLong(value, 0);
    }


    public long getLastEntryInSegment(long segmentId) throws IOException {
        return getLastEntryInSegmentInternal(segmentId);
    }

    /**
     * Get the offset of a segment's last entry.
     *
     * @param segmentId id of a segment
     * @return the offset of a segment's last entry
     * @throws IOException
     */
    private long getLastEntryInSegmentInternal(long segmentId) throws IOException {
        byte[] key = ByteUtil.convLong2Bytes(segmentId, Long.MAX_VALUE);
        // Search the last entry in storage
        Entry<byte[], byte[]> entry = positionsDb.getFloor(key);

        if (entry == null) {
            throw new IOException();
        } else {
            long foundLedgerId = ByteUtil.getLong(entry.getKey(), 0);
            long lastEntryId = ByteUtil.getLong(entry.getKey(), 8);

            if (foundLedgerId == segmentId) {
                if (log.isDebugEnabled()) {
                    log.debug("Found last page in storage db for ledger {} - last entry: {}.", segmentId, lastEntryId);
                }
                return lastEntryId;
            } else {
                throw new IOException();
            }
        }
    }

    public void addPosition(long segmentId, long entryId, long position) throws IOException {
        Batch batch = positionsDb.newBatch();
        addPosition(batch, segmentId, entryId, position);
        batch.flush();
        batch.close();
    }

    public Batch newBatch() {
        return positionsDb.newBatch();
    }

    /**
     * Add an index entry into the batch.
     *
     * @param batch     batch manager
     * @param segmentId id of segment
     * @param entryId   id of message entry
     * @param position  offset in the segment file.
     * @throws IOException
     */
    public void addPosition(Batch batch, long segmentId, long entryId, long position) throws IOException {
        byte[] key = ByteUtil.convLong2Bytes(segmentId, entryId);
        byte[] value = ByteUtil.convLong2Bytes(position);
        if (log.isDebugEnabled()) {
            log.debug("Add position index: segmentId: {}, entryId: {}, position: {}.", segmentId, entryId, position);
        }
        batch.put(key, value);
    }
}

