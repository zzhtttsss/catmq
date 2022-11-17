package org.catmq;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.catmq.remoting.protocol.RemotingCommand;

import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.concurrent.ConcurrentHashMap;

@ChannelHandler.Sharable
public class SimpleServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

    private static final ConcurrentHashMap<SocketAddress, Channel> channelTables = new ConcurrentHashMap<>();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        channelTables.put(channel.remoteAddress(), channel);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        channelTables.remove(channel.remoteAddress());
        System.out.println("channelGroup size" + channelTables.size());
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println(ctx.channel().remoteAddress() + " 上线了~");
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        System.out.println(ctx.channel().remoteAddress() + " 离线了~");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        System.out.println("Get cmd " + msg.toString());
    }
}