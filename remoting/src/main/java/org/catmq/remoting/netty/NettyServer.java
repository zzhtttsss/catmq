package org.catmq.remoting.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.catmq.remoting.InvokeCallback;
import org.catmq.remoting.RemotingServer;
import org.catmq.remoting.common.Pair;
import org.catmq.remoting.common.RemotingUtil;
import org.catmq.remoting.common.ThreadFactoryWithIndex;
import org.catmq.remoting.protocol.RemotingCommand;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * @author BYL
 */
public class NettyServer extends AbstractNettyRemoting implements RemotingServer {
    private final Logger log = Logger.getLogger(NettyServer.class.getCanonicalName());
    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup children;
    private final EventLoopGroup parents;
    private final ExecutorService publicExecutor;

    public NettyServer() {
        this.serverBootstrap = new ServerBootstrap();
        this.parents = new NioEventLoopGroup(1);
        this.children = new NioEventLoopGroup();
        this.publicExecutor = new ThreadPoolExecutor(4, 4,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new ThreadFactoryWithIndex("NettyServer_"));
    }

    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, publicExecutor);
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
        this.defaultRequestProcessorPair = new Pair<>(processor, executor);
    }


    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int requestCode) {
        return super.processorTable.get(requestCode);
    }

    @Override
    public Pair<NettyRequestProcessor, ExecutorService> getDefaultProcessorPair() {
        return defaultRequestProcessorPair;
    }

    @Override
    public RemotingCommand invokeSync(Channel channel, RemotingCommand request, long timeoutMillis) throws Exception {
        return this.invokeSyncImpl(channel, request, timeoutMillis);
    }

    @Override
    public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) throws Exception {
        this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
    }

    @Override
    public void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis) throws Exception {
        this.invokeOnewayImpl(channel, request, timeoutMillis);
    }

    @Override
    public void start() {
        serverBootstrap.group(this.parents, this.children)
                .channel(NioServerSocketChannel.class)
                .localAddress(8888)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new NettyEncoder(),
                                        new NettyDecoder(),
                                        new IdleStateHandler(0, 0, 120),
                                        new NettyServerHandler()
                                );
                    }
                });
        InetSocketAddress addr = new InetSocketAddress(8888);
        try {
            ChannelFuture sync = serverBootstrap.bind().sync();
            addr = (InetSocketAddress) sync.channel().localAddress();
            log.info(String.format("RemotingServer started, listening %s:%d", addr.getHostName(), addr.getPort()));
        } catch (Exception e) {
            throw new IllegalStateException(String.format("Failed to bind to %s:%d", addr.getHostName(), addr.getPort()), e);
        }
    }

    @Override
    public void shutdown() {
        this.parents.shutdownGracefully();
        this.children.shutdownGracefully();
        this.publicExecutor.shutdown();
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.publicExecutor;
    }

    @ChannelHandler.Sharable
    class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            log.info("Get command " + msg.toString());
            NettyServer.super.processMessageReceived(ctx, msg);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            String s = RemotingUtil.parseChannelRemoteAddr(ctx.channel());
            log.info(String.format("A new channel %s is active\n", s));
        }
    }
}
