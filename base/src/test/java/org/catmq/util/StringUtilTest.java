package org.catmq.util;

import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

public class StringUtilTest {
    @Test
    public void testParseAddress() {
        String address = "/address/broker/127.0.0.1:5432";
        InetSocketAddress socketAddress = StringUtil.parseAddress(address);
        Assert.assertEquals(5432, socketAddress.getPort());
        Assert.assertEquals("127.0.0.1", socketAddress.getHostName());
        System.out.println(socketAddress.getAddress().getHostAddress());
    }

    @Test
    public void testOffset2FileName() {
        String fileName = StringUtil.offset2FileName(1729);
        Assert.assertEquals("00000000000000001729", fileName);
        List<Integer> ll = List.of(1, 5, 7, 10, 16, 18, 22, 25);
        System.out.println(Collections.binarySearch(ll, 15));
        System.out.println(Collections.binarySearch(ll, 16));
        System.out.println(Collections.binarySearch(ll, 17));
    }
}
