package org.catmq.zk;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.catmq.command.BooleanError;
import org.catmq.constant.FileConstant;
import org.catmq.constant.ZkConstant;
import org.catmq.util.StringUtil;

import static org.catmq.storer.Storer.STORER;

@Slf4j
public class StorerZooKeeperClient extends BaseZookeeper {

    @Getter
    private final String storerPath;

    public StorerZooKeeperClient(String zkAddress) {
        super(zkAddress);
        this.storerPath = "/storer/" + STORER.getStorerInfo().getStorerId();
    }

    @Override
    public void register2Zk() {
        BooleanError res = registerStorerInfo();
        if (!res.isSuccess()) {
            log.error("Register broker info to zk failed. {}", res.getError());
            System.exit(-1);
        }
    }


    @Override
    protected void close() {

    }

    /**
     * This method is used to register all storer info into zookeeper
     * whose path is /storer and the data is {@code Storer}.
     *
     * @return BooleanError
     */
    private BooleanError registerStorerInfo() {
        try {
            //TODO: change mode to ephemeral to make test easier
            this.client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(this.storerPath, STORER.getStorerInfo().toBytes());
            this.client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(StringUtil.concatString(ZkConstant.TMP_STORER_PATH, FileConstant.LEFT_SLASH, STORER.getStorerInfo().getStorerId()));
        } catch (Exception e) {
            e.printStackTrace();
            return BooleanError.fail(e.getMessage());
        }
        return BooleanError.ok();
    }
}
