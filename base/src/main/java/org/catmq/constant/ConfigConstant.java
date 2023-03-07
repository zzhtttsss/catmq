package org.catmq.constant;

public class ConfigConstant {

    /**
     * Common constants
     */
    public static final String ZK_ADDRESS = "zk.address";


    /**
     * Broker constants
     */
    public static final String BROKER_IP = "broker.ip";
    public static final String BROKER_PORT = "broker.port";
    public static final String BROKER_NAME = "broker.name";
    public static final String GRPC_PRODUCER_THREAD_QUEUE_CAPACITY = "grpc.producer.threadQueueCapacity";
    public static final String GRPC_PRODUCER_THREAD_POOL_NUMS = "grpc.producer.threadPoolNums";
    public static final String GRPC_ADMIN_THREAD_POOL_NUMS = "grpc.admin.threadPoolNums";

    public static final String GRPC_CONSUMER_THREAD_QUEUE_CAPACITY = "grpc.consumer.threadQueueCapacity";
    public static final String GRPC_CONSUMER_THREAD_POOL_NUMS = "grpc.consumer.threadPoolNums";
    public static final String TOPIC_MAX_SEGMENT_MESSAGE_NUM = "topic.maxSegmentMessageNum";
    //TODO: config directory problem
    /**
     * broker config is in resource directory and producer config is in conf directory
     */
    public static final String BROKER_CONFIG_PATH = "/broker.properties";

    public static final String PRODUCER_CONFIG_PATH = "./conf/producer.properties";

    public static final String CONSUMER_CONFIG_PATH = "./conf/consumer.properties";
    /**
     * Storer constants
     */
    public static final String STORER_IP = "storer.ip";
    public static final String STORER_PORT = "storer.port";
    public static final String STORER_NAME = "storer.name";
    public static final String WRITE_ORDERED_EXECUTOR_THREAD_NUMS = "thread.writeOrderedExecutorThreadNums";
    public static final String READ_ORDERED_EXECUTOR_THREAD_NUMS = "thread.readOrderedExecutorThreadNums";
    public static final String NEED_WARM_MAPPED_FILE = "messageLog.needWarmMappedFile";
    public static final String FLUSH_MESSAGE_ENTRY_QUEUE_CAPACITY = "messageLog.flushMessageEntryQueueCapacity";
    public static final String MESSAGE_LOG_STORAGE_PATH = "messageLog.storagePath";
    public static final String MESSAGE_LOG_MAX_FILE_SIZE = "messageLog.maxFileSize";
    public static final String SEGMENT_STORAGE_PATH = "segment.storagePath";
    public static final String SEGMENT_MAX_FILE_SIZE = "segment.maxFileSize";
    public static final String SEGMENT_INDEX_STORAGE_PATH = "segment.indexStoragePath";
    public static final String MAX_SEGMENT_ENTRY_NUM = "segment.maxSegmentEntryNum";
    public static final String FLUSH_MODE = "messageLog.flushMode";

    public static final String READ_CACHE_EXPIRE_TIME = "readCache.expireTime";

    public static final String READ_CACHE_CLEAN_UP_INTERVAL = "readCache.cleanUpInterval";
    public static final String READ_CACHE_REMAINING_THRESHOLD = "readCache.remainingThreshold";
    public static final String TOPIC_MAX_READ_BATCH_SIZE = "topic.maxReadBatchSize";
    public static final String TOPIC_MAX_READ_CACHE_SIZE = "topic.maxReadCacheSize";
}
