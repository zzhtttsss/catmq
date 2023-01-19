package org.catmq.util;

import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;

public class StringUtilTest {
    @Test
    public void testParseAddress() {
        String address = "/address/broker/127.0.0.1:5432";
        InetSocketAddress socketAddress = StringUtil.parseAddress(address);
        Assert.assertEquals(5432, socketAddress.getPort());
        Assert.assertEquals("127.0.0.1", socketAddress.getHostName());
        System.out.println(socketAddress.getAddress().getHostAddress());
    }
}
