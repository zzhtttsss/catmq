package org.catmq.zk;

import cn.hutool.core.lang.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;

public class ZkUtilTest {
    @Test
    public void testGetFullBrokerAddressPath() {
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 8080);
        String path = ZkUtil.getFullBrokerAddressPath(address);
        Assert.equals(path, "/address/broker/127.0.0.1:8080");
    }
}
