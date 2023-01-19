package org.catmq.constant;

public class ConfigConstant {
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
}
