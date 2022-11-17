package org.catmq;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.catmq.remoting.protocol.RemotingCommand;

public class SimpleClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {


    protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        System.out.println(msg.toString());
    }
}
