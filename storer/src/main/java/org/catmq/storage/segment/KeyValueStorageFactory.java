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
        Default, // Used for default,command until or test case
        LedgerMetadata, // Used for ledgers db, doesn't need particular configuration
        EntryLocation // Used for location index, lots of writes and much bigger dataset
    }

    KeyValueStorage newKeyValueStorage(String defaultBasePath, String subPath, DbConfigType dbConfigType)
            throws IOException;
}
