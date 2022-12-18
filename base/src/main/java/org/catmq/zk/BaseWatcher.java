package org.catmq.zk;

import lombok.Builder;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;

/**
 * @author BYL
 */
@Builder(setterPrefix = "set")
public class BaseWatcher implements Watcher {
    @Builder.Default
    private Logger log = org.slf4j.LoggerFactory.getLogger(BaseWatcher.class);
    private INodeCreated nodeCreated;
    private INodeDeleted nodeDeleted;
    private INodeDataChanged nodeDataChanged;
    private INodeChildrenChanged nodeChildrenChanged;
    private INone none;
    private IRegisterWatcher registerWatcher;

    /**
     * This method is used to process the event from zookeeper.
     *
     * @param watchedEvent WatchedEvent passed by zookeeper
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        log.info("Get event : {}", watchedEvent.getType());
        switch (watchedEvent.getType()) {
            case NodeCreated -> {
                if (nodeCreated != null) {
                    nodeCreated.processNodeCreated(watchedEvent);
                }
            }
            case NodeDeleted -> {
                if (nodeDeleted != null) {
                    nodeDeleted.processNodeDeleted(watchedEvent);
                }
            }
            case NodeDataChanged -> {
                if (nodeDataChanged != null) {
                    nodeDataChanged.processNodeDataChanged(watchedEvent);
                }
            }
            case NodeChildrenChanged -> {
                if (nodeChildrenChanged != null) {
                    nodeChildrenChanged.processNodeChildrenChanged(watchedEvent);
                }
            }
            case None -> {
                if (none != null) {
                    none.processNone(watchedEvent);
                }
            }
            default -> log.error("Unknown event type : {}", watchedEvent.getType());
        }
    }
}
