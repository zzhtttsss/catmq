package org.catmq.util;

import org.apache.curator.framework.CuratorFramework;

public interface IdGenerator {
    /**
     * generate id
     *
     * @return id
     */
    long nextId(CuratorFramework client);
}
