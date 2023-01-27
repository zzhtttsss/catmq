package org.catmq.common;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class FileChannelWrapper implements AutoCloseable {
    private final RandomAccessFile randomAccessFile;
    private final FileChannel fileChannel;

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    @Override
    public void close() throws IOException {
        this.fileChannel.close();
        this.randomAccessFile.close();
    }

    public FileChannelWrapper(File file, String mode) throws FileNotFoundException {
        this.randomAccessFile = new RandomAccessFile(file, mode);
        this.fileChannel = this.randomAccessFile.getChannel();
    }
}
