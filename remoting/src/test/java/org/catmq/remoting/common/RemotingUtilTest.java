package org.catmq.remoting.common;

import io.netty.channel.Channel;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import static org.mockito.Mockito.mock;

public class RemotingUtilTest {

    @Test
    public void testString2SocketAddress() {
        SocketAddress localAddress = RemotingUtil.string2SocketAddress("127.0.0.1:8080");
        TestCase.assertTrue(localAddress instanceof InetSocketAddress);
        InetSocketAddress inet = (InetSocketAddress) localAddress;
        TestCase.assertEquals("127.0.0.1", inet.getHostString());
        TestCase.assertEquals(8080, inet.getPort());
    }

    @Test
    public void testSocketAddress2String() {
        SocketAddress localAddress = new InetSocketAddress("127.0.0.1", 8080);
        String addr = RemotingUtil.socketAddress2String(localAddress);
        TestCase.assertEquals("127.0.0.1:8080", addr);
    }

    @Test
    public void testParseChannelAddr() {
        String remote = "127.0.0.1:7070";
        String local = "127.0.0.1:50070";
        Channel channel = mock(Channel.class);
        Mockito.when(channel.localAddress()).thenReturn(RemotingUtil.string2SocketAddress(local));
        Mockito.when(channel.remoteAddress()).thenReturn(RemotingUtil.string2SocketAddress(remote));
        String s = RemotingUtil.parseChannelRemoteAddr(channel);
        Assert.assertEquals(remote, s);
        s = RemotingUtil.parseChannelLocalAddr(channel);
        Assert.assertEquals(local, s);
        Mockito.when(channel.localAddress()).thenReturn(null);
        Mockito.when(channel.remoteAddress()).thenReturn(null);
        s = RemotingUtil.parseChannelRemoteAddr(channel);
        Assert.assertEquals("", s);
        s = RemotingUtil.parseChannelLocalAddr(channel);
        Assert.assertEquals("", s);
    }

    @Test
    public void testCloseChannel() {
        Channel channel = mock(Channel.class);
        Mockito.when(channel.remoteAddress()).thenReturn(null);
        RemotingUtil.closeChannel(channel);
        Mockito.verify(channel, Mockito.times(1)).close();
    }
}
