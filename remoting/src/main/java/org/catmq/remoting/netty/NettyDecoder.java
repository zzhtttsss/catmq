package org.catmq.remoting.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.catmq.remoting.common.RemotingUtil;
import org.catmq.remoting.protocol.RemotingCommand;

import java.util.logging.Logger;

/**
 * @author BYL
 */
public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    private static final Logger log = Logger.getLogger(NettyDecoder.class.getCanonicalName());

    public NettyDecoder() {
        super(16777216, 0, 4, 0, 4);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try {
            frame = (ByteBuf) super.decode(ctx, in);
            if (null == frame) {
                return null;
            }
            return RemotingCommand.decode(frame);
        } catch (Exception e) {
            log.warning("decode exception, " + RemotingUtil.parseChannelRemoteAddr(ctx.channel()));
            RemotingUtil.closeChannel(ctx.channel());
        } finally {
            if (null != frame) {
                frame.release();
            }
        }
        return null;
    }
}
