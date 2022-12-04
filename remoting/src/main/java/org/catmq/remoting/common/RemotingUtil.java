package org.catmq.remoting.common;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.logging.Logger;

/**
 * @author BYL
 */
public class RemotingUtil {
    private static final Logger log = Logger.getLogger(RemotingUtil.class.getCanonicalName());

    public static SocketAddress string2SocketAddress(final String addr) {
        int split = addr.lastIndexOf(":");
        String host = addr.substring(0, split);
        String port = addr.substring(split + 1);
        return new InetSocketAddress(host, Integer.parseInt(port));
    }

    public static String socketAddress2String(final SocketAddress addr) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) addr;
        return inetSocketAddress.getAddress().getHostAddress() + ":" +
                inetSocketAddress.getPort();
    }

    public static void closeChannel(Channel channel) {
        final String addrRemote = parseChannelRemoteAddr(channel);
        if ("".equals(addrRemote)) {
            channel.close();
        } else {
            channel.close().addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    log.info(String.format("closeChannel: close the connection to remote address[%s] result: %s", addrRemote,
                            future.isSuccess()));
                }
            });
        }
    }

    public static String parseChannelRemoteAddr(final Channel channel) {
        SocketAddress remote = channel.remoteAddress();
        if (remote != null) {
            return RemotingUtil.socketAddress2String(remote);
        }
        return "";
    }

    public static String parseChannelLocalAddr(final Channel channel) {
        SocketAddress remote = channel.localAddress();
        if (remote != null) {
            return RemotingUtil.socketAddress2String(remote);
        }
        return "";
    }
}
