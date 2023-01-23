package org.catmq.storage.segment;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.catmq.storer.StorerConfig.STORER_CONFIG;

public class EntryPositionIndexTest {

    static EntryPositionIndex entryPositionIndex;

    @BeforeClass
    public static void beforeClass() {
        STORER_CONFIG.setSegmentIndexStoragePath(Path.of("./src/index").toAbsolutePath().normalize().toString());
        entryPositionIndex = new EntryPositionIndex(KeyValueStorageRocksDB.factory,
                STORER_CONFIG.getSegmentIndexStoragePath());
    }

    @AfterClass
    public static void afterClass() throws IOException {
        entryPositionIndex.close();
    }

    @Test
    public void testAddPosition() throws IOException {
        entryPositionIndex.addPosition(1, 1, 1);
        System.out.println(entryPositionIndex.getPosition(1, 1));
    }
}
