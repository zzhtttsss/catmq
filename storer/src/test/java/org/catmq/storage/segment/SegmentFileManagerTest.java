package org.catmq.storage.segment;

import org.catmq.common.FileChannelWrapper;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static org.catmq.storer.StorerConfig.STORER_CONFIG;

public class SegmentFileManagerTest {

    static SegmentFileManager service;
    static File tmp;

    @BeforeClass
    public static void beforeClass() throws IOException {
        STORER_CONFIG.setSegmentStoragePath(Path.of("./src/segment").toAbsolutePath().normalize().toString());
        service = SegmentFileManager.SegmentFileServiceEnum.INSTANCE.getInstance();
    }


    @Test
    public void testGetSegmentFileOffsetByOffset() {
        Long fileOffset = service.getSegmentFileOffsetByOffset(82000L);
        Assert.assertEquals(0L, fileOffset.longValue());
    }

    @Test
    public void testGetOrCreateSegmentFileByOffset() {
        try (FileChannelWrapper wrapper = service.getOrCreateSegmentFileByOffset(0L, false)) {
            Assert.assertNotNull(wrapper);
            Assert.assertNotNull(wrapper.getFileChannel());
            Assert.assertEquals(1, service.getPaths().size());
            var buf = wrapper.getFileChannel().map(FileChannel.MapMode.READ_ONLY, 82000, 4 * 1024 * 1024);
            Assert.assertNotNull(buf);
        } catch (IOException e) {
            Assert.fail();
        }
    }
}
