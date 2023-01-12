package org.catmq.storage.segment;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map.Entry;

import org.catmq.storage.segment.KeyValueStorage.*;
import org.catmq.util.ByteUtil;

@Slf4j
public class EntryOffsetIndex implements Closeable {

    private KeyValueStorage locationsDb;

    public EntryOffsetIndex(KeyValueStorageFactory storageFactory, String basePath) {
        try {
            locationsDb = storageFactory.newKeyValueStorage(basePath, "locations",
                    KeyValueStorageFactory.DbConfigType.EntryLocation);
        } catch (IOException e) {
            locationsDb = null;
            log.error("fail to build locationsDb", e);
        }
    }

    @Override
    public void close() throws IOException {
        locationsDb.close();
    }

    public long getLocation(long ledgerId, long entryId) throws IOException {
        byte[] key = ByteUtil.convLong2Bytes(ledgerId, entryId);
        byte[] value = new byte[Long.BYTES];
        if (locationsDb.get(key, value) < 0) {
            return 0;
        }
        return ByteUtil.getLong(value, 0);
    }


    public long getLastEntryInLedger(long ledgerId) throws IOException {
        return getLastEntryInLedgerInternal(ledgerId);
    }

    private long getLastEntryInLedgerInternal(long ledgerId) throws IOException {
        byte[] key = ByteUtil.convLong2Bytes(ledgerId, Long.MAX_VALUE);
        // Search the last entry in storage
        Entry<byte[], byte[]> entry = locationsDb.getFloor(key);

        if (entry == null) {
            throw new IOException();
        } else {
            long foundLedgerId = ByteUtil.getLong(entry.getKey(), 0);
            long lastEntryId = ByteUtil.getLong(entry.getKey(), 8);

            if (foundLedgerId == ledgerId) {
                if (log.isDebugEnabled()) {
                    log.debug("Found last page in storage db for ledger {} - last entry: {}", ledgerId, lastEntryId);
                }
                return lastEntryId;
            } else {
                throw new IOException();
            }
        }
    }

    public void addLocation(long ledgerId, long entryId, long location) throws IOException {
        Batch batch = locationsDb.newBatch();
        addLocation(batch, ledgerId, entryId, location);
        batch.flush();
        batch.close();
    }

    public Batch newBatch() {
        return locationsDb.newBatch();
    }

    public void addLocation(Batch batch, long ledgerId, long entryId, long location) throws IOException {
        byte[] key = ByteUtil.convLong2Bytes(ledgerId, entryId);
        byte[] value = ByteUtil.convLong2Bytes(location);

        if (log.isDebugEnabled()) {
            log.debug("Add location - ledger: {} -- entry: {} -- location: {}", ledgerId, entryId, location);
        }
        batch.put(key, value);
    }

//    public void updateLocations(Iterable<EntryLocation> newLocations) throws IOException {
//        if (log.isDebugEnabled()) {
//            log.debug("Update locations -- {}", Iterables.size(newLocations));
//        }
//
//        Batch batch = newBatch();
//        // Update all the ledger index pages with the new locations
//        for (EntryLocation e : newLocations) {
//            if (log.isDebugEnabled()) {
//                log.debug("Update location - ledger: {} -- entry: {}", e.ledger, e.entry);
//            }
//
//            addLocation(batch, e.ledger, e.entry, e.location);
//        }
//
//        batch.flush();
//        batch.close();
//    }

//    public void delete(long ledgerId) throws IOException {
//        // We need to find all the LedgerIndexPage records belonging to one specific
//        // ledgers
//        deletedLedgers.add(ledgerId);
//    }

//    public void removeOffsetFromDeletedLedgers() throws IOException {
//        LongPairWrapper firstKeyWrapper = LongPairWrapper.get(-1, -1);
//        LongPairWrapper lastKeyWrapper = LongPairWrapper.get(-1, -1);
//
//        Set<Long> ledgersToDelete = deletedLedgers.items();
//
//        if (ledgersToDelete.isEmpty()) {
//            return;
//        }
//
//        log.info("Deleting indexes for ledgers: {}", ledgersToDelete);
//        long startTime = System.nanoTime();
//
//        try (Batch batch = locationsDb.newBatch()) {
//            for (long ledgerId : ledgersToDelete) {
//                if (log.isDebugEnabled()) {
//                    log.debug("Deleting indexes from ledger {}", ledgerId);
//                }
//
//                firstKeyWrapper.set(ledgerId, 0);
//                lastKeyWrapper.set(ledgerId, Long.MAX_VALUE);
//
//                batch.deleteRange(firstKeyWrapper.array, lastKeyWrapper.array);
//            }
//
//            batch.flush();
//            for (long ledgerId : ledgersToDelete) {
//                deletedLedgers.remove(ledgerId);
//            }
//        } finally {
//            firstKeyWrapper.recycle();
//            lastKeyWrapper.recycle();
//        }
//
//        log.info("Deleted indexes from {} ledgers in {} seconds", ledgersToDelete.size(),
//                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) / 1000.0);
//    }
}

