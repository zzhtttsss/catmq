package org.catmq.storer;

import io.netty.util.internal.PlatformDependent;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.catmq.constant.ConfigConstant;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.catmq.constant.FileConstant.MB;
import static org.catmq.util.ConfigUtil.PROCESSOR_NUMBER;

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
    private static final String CONFIG_PATH = "src/main/resources/storer.properties";

    @Getter@Setter
    private String storerName;
    @Getter@Setter
    private String storerIp;
    @Getter@Setter
    private int storerPort;

    @Getter@Setter
    private int writeOrderedExecutorThreadNums;
    @Getter@Setter
    private int readOrderedExecutorThreadNums;
    @Getter@Setter
    private boolean needWarmMappedFile;
    @Getter@Setter
    private int flushMessageEntryQueueCapacity;
    @Getter@Setter
    private String messageLogStoragePath;
    @Getter@Setter
    private int messageLogMaxFileSize;
    @Getter@Setter
    private String segmentStoragePath;
    @Getter@Setter
    private long segmentMaxFileSize;

    public void readConfig() {
        InputStream stream = this.getClass().getResourceAsStream(CONFIG_PATH);
        if (stream == null) {
            throw new RuntimeException("broker.properties not found");
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
        writeOrderedExecutorThreadNums = Integer.parseInt(properties.getProperty(ConfigConstant.WRITE_ORDERED_EXECUTOR_THREAD_NUMS, "1"));
        readOrderedExecutorThreadNums = Integer.parseInt(properties.getProperty(ConfigConstant.READ_ORDERED_EXECUTOR_THREAD_NUMS,
                        String.valueOf(PROCESSOR_NUMBER)));
        needWarmMappedFile = Boolean.parseBoolean(properties.getProperty(ConfigConstant.NEED_WARM_MAPPED_FILE, "false"));
        flushMessageEntryQueueCapacity = Integer.parseInt(properties.getProperty(ConfigConstant.FLUSH_MESSAGE_ENTRY_QUEUE_CAPACITY, "10000"));
        messageLogStoragePath = properties.getProperty(ConfigConstant.MESSAGE_LOG_STORAGE_PATH, "src/messageLog");
        messageLogMaxFileSize = (int) (Integer.parseInt(properties.getProperty(ConfigConstant.MESSAGE_LOG_MAX_FILE_SIZE,
                "1024")) * MB);
        segmentStoragePath = properties.getProperty(ConfigConstant.MESSAGE_LOG_STORAGE_PATH, "src/segment");
        segmentMaxFileSize = Long.parseLong(properties.getProperty(ConfigConstant.MESSAGE_LOG_MAX_FILE_SIZE,
                String.valueOf(0.25 * PlatformDependent.estimateMaxDirectMemory())));
    }
}
