package org.catmq.entity;

import io.netty.util.internal.PlatformDependent;
import lombok.Getter;
import lombok.Setter;
import org.catmq.constant.ConfigConstant;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.catmq.constant.FileConstant.MB;
import static org.catmq.util.ConfigUtil.PROCESSOR_NUMBER;

@Getter
@Setter
public class StorerConfig {

    public static StorerConfig STORER_CONFIG;

    static {
        STORER_CONFIG = new StorerConfig();
    }

    private StorerConfig() {
        readConfig();
    }

    public static final String WRITE_ORDERED_EXECUTOR_NAME = "writeOrderedExecutor";
    public static final String READ_ORDERED_EXECUTOR_NAME = "readOrderedExecutor";
    private static final String CONFIG_PATH = "/storer.properties";


    private String storerName;
    private String storerIp;
    private int storerPort;

    private int writeOrderedExecutorThreadNums;
    private int readOrderedExecutorThreadNums;
    private boolean needWarmMappedFile;
    private int flushMessageEntryQueueCapacity;
    private String messageLogStoragePath;
    private int messageLogMaxFileSize;
    private String segmentStoragePath;
    private long segmentMaxFileSize;
    private String segmentIndexStoragePath;
    private long maxSegmentEntryNum;
    private FlushMode flushMode;

    private long readCacheExpireTime;

    private long readCacheCleanUpInterval;
    private float readCacheRemainingThreshold;

    public void readConfig() {
        InputStream stream = this.getClass().getResourceAsStream(CONFIG_PATH);
        if (stream == null) {
            throw new RuntimeException("storer.properties not found");
        }
        Properties properties = new Properties();
        try {
            properties.load(stream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        storerName = properties.getProperty(ConfigConstant.STORER_NAME, "default");
        storerIp = properties.getProperty(ConfigConstant.STORER_IP, "127.0.0.1");
        storerPort = Integer.parseInt(properties.getProperty(ConfigConstant.STORER_PORT, "4321"));
        writeOrderedExecutorThreadNums = Integer.parseInt(properties.getProperty(ConfigConstant.WRITE_ORDERED_EXECUTOR_THREAD_NUMS, "4"));
        readOrderedExecutorThreadNums = Integer.parseInt(properties.getProperty(ConfigConstant.READ_ORDERED_EXECUTOR_THREAD_NUMS,
                String.valueOf(PROCESSOR_NUMBER)));
        needWarmMappedFile = Boolean.parseBoolean(properties.getProperty(ConfigConstant.NEED_WARM_MAPPED_FILE, "false"));
        flushMessageEntryQueueCapacity = Integer.parseInt(properties.getProperty(ConfigConstant.FLUSH_MESSAGE_ENTRY_QUEUE_CAPACITY, "100000"));
        messageLogStoragePath = properties.getProperty(ConfigConstant.MESSAGE_LOG_STORAGE_PATH, "storer/src/messageLog");
        messageLogMaxFileSize = (int) (Integer.parseInt(properties.getProperty(ConfigConstant.MESSAGE_LOG_MAX_FILE_SIZE,
                "1024")) * MB);
        segmentStoragePath = properties.getProperty(ConfigConstant.SEGMENT_STORAGE_PATH, "storer/src/segment");
        segmentMaxFileSize = Long.parseLong(properties.getProperty(ConfigConstant.SEGMENT_MAX_FILE_SIZE,
                String.valueOf(0.25 * PlatformDependent.estimateMaxDirectMemory())));
        segmentIndexStoragePath = properties.getProperty(ConfigConstant.SEGMENT_INDEX_STORAGE_PATH, "storer/src/index");
        maxSegmentEntryNum = Long.parseLong(properties.getProperty(ConfigConstant.MAX_SEGMENT_ENTRY_NUM, String.valueOf(100000)));
        readCacheExpireTime = Long.parseLong(properties.getProperty(ConfigConstant.READ_CACHE_EXPIRE_TIME, String.valueOf(5000)));
        readCacheCleanUpInterval = Long.parseLong(properties.getProperty(ConfigConstant.READ_CACHE_CLEAN_UP_INTERVAL, String.valueOf(60000)));
        readCacheRemainingThreshold = Float.parseFloat(properties.getProperty(ConfigConstant.READ_CACHE_REMAINING_THRESHOLD, String.valueOf(0.9)));
        flushMode = FlushMode.fromString(properties.getProperty(ConfigConstant.FLUSH_MODE, "async"));

    }
}
