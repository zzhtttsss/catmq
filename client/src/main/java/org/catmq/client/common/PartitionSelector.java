package org.catmq.client.common;

public interface PartitionSelector {

    void selectPartition(MessageEntry messageEntry);
}
