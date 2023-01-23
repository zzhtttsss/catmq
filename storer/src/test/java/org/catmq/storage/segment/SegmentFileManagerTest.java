package org.catmq.storage.segment;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.catmq.storer.StorerConfig.STORER_CONFIG;

public class SegmentFileManagerTest {

    static SegmentFileManager service;
    static File tmp;

    @BeforeClass
    public static void beforeClass() throws IOException {
        STORER_CONFIG.setSegmentStoragePath(Path.of("./src/segment").toAbsolutePath().normalize().toString());
        tmp = new File(STORER_CONFIG.getSegmentStoragePath() + "/0000000120");
        tmp.createNewFile();
        service = SegmentFileManager.SegmentFileServiceEnum.INSTANCE.getInstance();
    }

    @Test
    public void testSegmentFileService() {
        Assert.assertEquals(1, service.getPaths().size());
        Assert.assertEquals(Long.valueOf(120L), service.getPaths().get(0));
    }

    @Test
    public void testsfs() {
        haha();
        System.out.println("haha");
    }

    private void haha() throws RuntimeException {
        try {
            System.out.println(1 / 0);
        } catch (Exception e) {
            throw new RuntimeException("haha");
        }
    }

    @AfterClass
    public static void afterClass() {
        tmp.delete();
    }
}
