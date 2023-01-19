package org.catmq.constant;

import java.io.RandomAccessFile;

/**
 * @author BYL
 */
public class FileConstant {
    public static final String INDEX = "index";
    public static final String COMMIT_LOG = "commitlog";
    public static final String CONSUME_QUEUE = "consumequeue";

    public static final String INDEX_PATH = "index.path";
    public static final String COMMIT_LOG_PATH = "commitlog.path";
    public static final String CONSUME_QUEUE_PATH = "consumequeue.path";

    public static final long KB = 1024;

    public static final long MB = 1024 * KB;

    public static final long GB = 1024 * MB;

    public static final long COMMIT_LOG_SIZE = MB;

    public static final String LEFT_SLASH = "/";

    public static final String Colon = ":";

    /**
     * Mode of {@link RandomAccessFile}
     */
    public static final String RANDOM_ACCESS_FILE_READ_MODE = "r";
    public static final String RANDOM_ACCESS_FILE_READ_WRITE_MODE = "rw";
    public static final String RANDOM_ACCESS_FILE_READ_WRITE_SYNC_MODE = "rws";
    public static final String RANDOM_ACCESS_FILE_READ_WRITE_DSYNC_MODE = "rwd";
    public static final String COLON = ":";
}
