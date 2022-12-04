package org.catmq.remoting.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.catmq.remoting.common.RemotingUtil;
import org.catmq.remoting.protocol.RemotingCommand;

import java.util.logging.Logger;


@ChannelHandler.Sharable
public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {
    private static final Logger log = Logger.getLogger(NettyEncoder.class.getCanonicalName());

    @Override
    public void encode(ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out) {
        try {
            remotingCommand.fastEncodeHeader(out);
            byte[] body = remotingCommand.getBody();
            if (body != null) {
                out.writeBytes(body);
            }
        } catch (Exception e) {
            log.warning("encode exception, " + RemotingUtil.parseChannelRemoteAddr(ctx.channel()));
            if (remotingCommand != null) {
                log.warning(remotingCommand.toString());
            }
            RemotingUtil.closeChannel(ctx.channel());
        }
    }
}
