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

    public static final String ZK_ADDRESS = "zk.address";
    //TODO: config directory problem
    /**
     * broker config is in resource directory and producer config is in conf directory
     */
    public static final String BROKER_CONFIG_PATH = "/broker.properties";

    public static final String PRODUCER_CONFIG_PATH = "./conf/producer.properties";

    public static final String CONSUMER_CONFIG_PATH = "./conf/consumer.properties";
    public static final String PRODUCER_CONFIG_PATH = "../conf/producer.properties";
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
}
