package org.catmq.storage.segment;

import java.io.IOException;

/**
 * Factory class to create instances of the key-value storage implementation.
 */
public interface KeyValueStorageFactory {

    /**
     * Enum used to specify different config profiles in the underlying storage.
     */
    enum DbConfigType {
        Default, // Used for default,command util or test case
        SegmentMetadata, // Used for ledgers db, doesn't need particular configuration
        EntryPosition // Used for location index, lots of writes and much bigger dataset
    }

    KeyValueStorage newKeyValueStorage(String defaultBasePath, String subPath, DbConfigType dbConfigType)
            throws IOException;
}
