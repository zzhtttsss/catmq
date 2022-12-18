package org.catmq.broker.zk;

import org.apache.zookeeper.WatchedEvent;
import org.catmq.zk.INone;
import org.slf4j.Logger;

import java.util.List;

/**
 * @author BYL
 */
public class DeadNodeWatcher implements INone {
    private Logger log = org.slf4j.LoggerFactory.getLogger(DeadNodeWatcher.class);
    private final BaseZooKeeper bzk;

    @Override
    public void processNone(WatchedEvent watchedEvent) {
        log.warn("Dead node {} {}. Try to broadcast others to handle it.", watchedEvent.getPath(), watchedEvent.getState());
        List<String> allBrokerPaths = this.bzk.getAllBrokerPaths(true);
        allBrokerPaths.forEach(path -> {
            log.warn("Broker {} is informed.", path);
        });
    }

    public DeadNodeWatcher(BaseZooKeeper bzk) {
        this.bzk = bzk;
    }
}
