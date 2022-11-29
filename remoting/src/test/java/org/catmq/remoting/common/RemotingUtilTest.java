package org.catmq.remoting.common;

import org.junit.Assert;
import org.junit.Test;

import java.net.SocketAddress;

public class RemotingUtilTest {

    @Test
    public void testStringAndSocketAddress() {
        SocketAddress localAddress = RemotingUtil.string2SocketAddress("127.0.0.1:8080");
        String s = RemotingUtil.socketAddress2String(localAddress);
        Assert.assertEquals("127.0.0.1:8080", s);
    }
}
