package org.catmq.zk;

import lombok.Getter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.zookeeper.CreateMode;
import org.catmq.constant.FileConstant;
import org.catmq.constant.ZkConstant;
import org.catmq.util.IdGenerator;
import org.catmq.util.StringUtil;

public class ZkIdGenerator implements IdGenerator {

    private static final String ID_SUFFIX = "-id-";

    @Override
    public long nextId(CuratorFramework client) {
        String path;
        try {
            if (client.getState() != CuratorFrameworkState.STARTED) {
                throw new RuntimeException("Zk client is not started");
            }
            path = client.create()
                    .creatingParentsIfNeeded()
                    .withProtection()
                    .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath(StringUtil.concatString(ZkConstant.UNIQUE_ID_PATH, FileConstant.LEFT_SLASH, "id-"));
            return Long.parseLong(StringUtil.substringAfterLast(path, ID_SUFFIX));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ZkIdGenerator() {
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
