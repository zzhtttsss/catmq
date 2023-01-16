package org.catmq.storer;

import lombok.Getter;
import lombok.Setter;

import static org.catmq.util.ConfigUtil.PROCESSOR_NUMBER;

public class StorerConfig {

    private StorerConfig() {
    }

    public static final String WRITE_ORDERED_EXECUTOR_NAME = "writeOrderedExecutor";

    public static final String READ_ORDERED_EXECUTOR_NAME = "readOrderedExecutor";

    @Getter
    @Setter
    private int writeOrderedExecutorThreadNums = 4;

    @Getter
    @Setter
    private int readOrderedExecutorThreadNums = PROCESSOR_NUMBER;




    public enum StorerConfigEnum {
        INSTANCE;
        private final StorerConfig brokerConfig;

        StorerConfigEnum() {
            brokerConfig = new StorerConfig();
        }

        public StorerConfig getInstance() {
            return brokerConfig;
        }
    }
}
