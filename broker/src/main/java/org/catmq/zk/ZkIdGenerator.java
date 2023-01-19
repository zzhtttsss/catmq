package org.catmq.zk;

import lombok.Getter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.zookeeper.CreateMode;
import org.catmq.broker.BrokerConfig;
import org.catmq.constant.FileConstant;
import org.catmq.constant.ZkConstant;
import org.catmq.util.IdGenerator;
import org.catmq.util.StringUtil;

public class ZkIdGenerator implements IdGenerator {

    private static final String ID_SUFFIX = "-id-";
    private final CuratorFramework client;

    @Override
    public long nextId() {
        String path = null;
        try {
            if (client.getState() != CuratorFrameworkState.STARTED) {
                throw new RuntimeException("Zk client is not started");
            }
            path = client.create()
                    .creatingParentsIfNeeded()
                    .withProtection()
                    .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath(StringUtil.concatString(ZkConstant.BROKER_ID_PATH, FileConstant.LEFT_SLASH, "id-"));
            return Long.parseLong(StringUtil.substringAfterLast(path, ID_SUFFIX));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ZkIdGenerator() {
        BrokerConfig config = BrokerConfig.BrokerConfigEnum.INSTANCE.getInstance();
        this.client = ZkUtil.createClient(config.getZkAddress());
    }

    public enum ZkIdGeneratorEnum {
        /**
         * Singleton instance.
         */
        INSTANCE;

        @Getter
        private final ZkIdGenerator instance;

        ZkIdGeneratorEnum() {
            instance = new ZkIdGenerator();
        }

    }
}
