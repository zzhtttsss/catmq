package org.catmq.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class BaseWatcherTest {
    @Test
    public void testProcess() {
        INodeCreated nodeCreated = watchedEvent -> {
            Assert.assertEquals(Watcher.Event.EventType.NodeCreated, watchedEvent.getType());
        };
        INodeDeleted nodeDeleted = Mockito.mock(INodeDeleted.class);
        INodeDataChanged nodeDataChanged = Mockito.mock(INodeDataChanged.class);
        INodeChildrenChanged nodeChildrenChanged = Mockito.mock(INodeChildrenChanged.class);
        INone none = Mockito.mock(INone.class);
        BaseWatcher bw = BaseWatcher
                .builder()
                .setNodeCreated(nodeCreated)
                .setNodeDeleted(nodeDeleted)
                .setNodeChildrenChanged(nodeChildrenChanged)
                .setNodeDataChanged(nodeDataChanged)
                .setNone(none)
                .build();
        WatchedEvent we = Mockito.mock(WatchedEvent.class);
        Mockito.when(we.getType()).thenReturn(Watcher.Event.EventType.NodeCreated);
        bw.process(we);
        Mockito.verify(nodeDeleted, Mockito.times(0)).processNodeDeleted(Mockito.any());
        Mockito.verify(none, Mockito.times(0)).processNone(Mockito.any());
        Mockito.verify(nodeDataChanged, Mockito.times(0)).processNodeDataChanged(Mockito.any());
        Mockito.verify(nodeChildrenChanged, Mockito.times(0)).processNodeChildrenChanged(Mockito.any());
    }
}
