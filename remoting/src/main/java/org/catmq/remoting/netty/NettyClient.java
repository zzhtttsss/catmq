package org.catmq.remoting.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.catmq.remoting.InvokeCallback;
import org.catmq.remoting.RemotingClient;
import org.catmq.remoting.common.Pair;
import org.catmq.remoting.common.RemotingUtil;
import org.catmq.remoting.protocol.RemotingCommand;
import org.catmq.thread.ThreadFactoryWithIndex;

import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * @author BYL
 */
public class NettyClient extends AbstractNettyRemoting implements RemotingClient {
    private final Logger log = Logger.getLogger(NettyClient.class.getCanonicalName());
    private final Bootstrap bootstrap;
    private final EventLoopGroup eventLoopGroupWorker;

    private final ScheduledThreadPoolExecutor timerExecutor;
    /**
     * all threads will be put into publicExecutor
     */
    private final ExecutorService publicExecutor;
    /**
     * callbackExecutor executes the call-back functions
     */
    private ExecutorService callbackExecutor;

    /**
     * channelTables is a concurrent hash map whose key is remote address and value is its channel
     */
    private final ConcurrentMap<String, Channel> channelTables = new ConcurrentHashMap<>();

    public NettyClient() {
        this.bootstrap = new Bootstrap();
        this.eventLoopGroupWorker = new NioEventLoopGroup(1,
                new ThreadFactoryWithIndex("NettyClientSelector_"));
        this.publicExecutor = new ThreadPoolExecutor(4, 4, 0L,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(), new ThreadFactoryWithIndex("publicThread_"));
        this.timerExecutor = new ScheduledThreadPoolExecutor(1,
                new ThreadFactoryWithIndex("NettyClientTimerThread_"));

    }

    @Override
    public void start() {
        this.bootstrap.group(this.eventLoopGroupWorker)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(
                                new NettyEncoder(),
                                new NettyDecoder(),
                                new IdleStateHandler(0, 0, 120),
                                new NettyClientHandler()
                        );
                    }
                });
        this.callbackExecutor = new ThreadPoolExecutor(3, 3, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new ThreadFactoryWithIndex("CallbackThread_"));
        this.timerExecutor.scheduleWithFixedDelay(new TimerTask() {
            @Override
            public void run() {
                try {
                    NettyClient.this.scanResponseTable();
                } catch (Exception e) {
                    log.warning("scanResponseTable exception");
                }
            }
        }, 1000 * 3, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        this.eventLoopGroupWorker.shutdownGracefully();
        if (this.callbackExecutor != null) {
            try {
                this.callbackExecutor.shutdown();
            } catch (Exception e) {
                log.warning("NettyClient callbackExecutor shutdown exception, " + e);
            }
        }
        try {
            this.publicExecutor.shutdown();
        } catch (Exception e) {
            log.warning("NettyClient publicExecutor shutdown exception, " + e);
        }
        try {
            this.timerExecutor.shutdown();
        } catch (Exception e) {
            log.warning("NettyClient timerExecutor shutdown exception, " + e);
        }
    }

    @Override
    public ExecutorService getCallbackExecutor() {
        return this.callbackExecutor;
    }

    @Override
    public RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis) throws Exception {
        long beginStartTime = System.currentTimeMillis();
        final Channel channel = this.getOrCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                this.doBeforeRpcHooks(addr, request);
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    throw new Exception("invokeSync call the addr[" + addr + "] timeout");
                }
                RemotingCommand response = this.invokeSyncImpl(channel, request, timeoutMillis - costTime);
                this.doAfterRpcHooks(addr, request, response);
                return response;
            } catch (Exception e) {
                log.warning("invokeSync: send request exception, so close the channel " + addr);
                RemotingUtil.closeChannel(channel);
                throw e;
            }
        } else {
            RemotingUtil.closeChannel(channel);
            throw new Exception(addr);
        }
    }

    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) throws Exception {
        long beginStartTime = System.currentTimeMillis();
        final Channel channel = this.getOrCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                this.doBeforeRpcHooks(addr, request);
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    throw new Exception("invokeAsync call the addr[" + addr + "] timeout");
                }
                this.invokeAsyncImpl(channel, request, timeoutMillis - costTime, new InvokeCallbackWrapper(invokeCallback, addr));
            } catch (Exception e) {
                log.warning("invokeAsync: send request exception, so close the channel " + addr);
                RemotingUtil.closeChannel(channel);
                throw e;
            }
        } else {
            RemotingUtil.closeChannel(channel);
            throw new Exception(addr);
        }
    }

    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws Exception {
        Channel channel = getOrCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                this.doBeforeRpcHooks(addr, request);
                this.invokeOnewayImpl(channel, request, timeoutMillis);
            } catch (Exception e) {
                log.warning("invokeOneway: send request exception, so close the channel " + addr);
                RemotingUtil.closeChannel(channel);
                throw e;
            }
        } else {
            log.warning(String.format("%s has not been put into channelTable yet", addr));
        }
    }

    /**
     * there will be lots of ExecutorService in future
     *
     * @param requestCode request code from request
     * @param processor
     * @param executor
     */
    @Override
    public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
        ExecutorService executorThis = executor;
        if (null == executor) {
            executorThis = this.publicExecutor;
        }
        Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<>(processor, executorThis);
        this.processorTable.put(requestCode, pair);
    }

    @Override
    public void setCallbackExecutor(ExecutorService callbackExecutor) {
        this.callbackExecutor = callbackExecutor;
    }

    @Override
    public void closeChannels(List<String> addrList) {
        addrList.forEach(addr -> {
            Channel channel = channelTables.get(addr);
            RemotingUtil.closeChannel(channel);
        });
    }

    private Channel getOrCreateChannel(final String addr) throws InterruptedException {
        Channel cw = this.channelTables.get(addr);
        if (cw != null) {
            return cw;
        }
        ChannelFuture future = this.bootstrap.connect(RemotingUtil.string2SocketAddress(addr)).sync();
        this.channelTables.put(addr, future.channel());
        return future.channel();
    }

    @ChannelHandler.Sharable
    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }
}
