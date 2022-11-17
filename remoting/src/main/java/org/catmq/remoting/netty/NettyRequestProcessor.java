package org.catmq.remoting.netty;

import io.netty.channel.ChannelHandlerContext;
import org.catmq.remoting.protocol.RemotingCommand;

/**
 * Common remoting command processor
 */
public interface NettyRequestProcessor {
    RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
            throws Exception;

    boolean rejectRequest();
}
