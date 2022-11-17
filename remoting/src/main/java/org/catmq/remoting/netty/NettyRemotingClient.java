package org.catmq.remoting.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.catmq.remoting.InvokeCallback;
import org.catmq.remoting.RemotingClient;
import org.catmq.remoting.common.NewThreadWithIndex;
import org.catmq.remoting.common.Pair;
import org.catmq.remoting.common.RemotingHelper;
import org.catmq.remoting.common.RemotingUtil;
import org.catmq.remoting.protocol.RemotingCommand;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class NettyRemotingClient extends NettyRemotingAbstract implements RemotingClient {

    private static final long LOCK_TIMEOUT_MILLIS = 3000;
    private static final InternalLogger LOGGER = InternalLoggerFactory.getInstance(RemotingHelper.ROCKETMQ_REMOTING);

    private final Bootstrap bootstrap = new Bootstrap();
    private final EventLoopGroup eventLoopGroupWorker;
    private final Lock lockChannelTables = new ReentrantLock();
    private final ConcurrentHashMap<String /* cidr */, Bootstrap> bootstrapMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String /* addr */, ChannelWrapper> channelTables = new ConcurrentHashMap<>();

    private final Timer timer = new Timer("ClientHouseKeepingService", true);

    private final AtomicReference<List<String>> namesrvAddrList = new AtomicReference<>();
    private final ConcurrentMap<String, Boolean> availableNamesrvAddrMap = new ConcurrentHashMap<>();
    private final AtomicReference<String> namesrvAddrChoosed = new AtomicReference<>();
    private final AtomicInteger namesrvIndex = new AtomicInteger(initValueIndex());
    private final Lock namesrvChannelLock = new ReentrantLock();

    private final ExecutorService publicExecutor;
    private final ExecutorService scanExecutor;

    /**
     * Invoke the callback methods in this executor when process response.
     */
    private ExecutorService callbackExecutor;
    private EventExecutorGroup defaultEventExecutorGroup;


    public NettyRemotingClient(final EventLoopGroup eventLoopGroup,
                               final EventExecutorGroup eventExecutorGroup) {

        int publicThreadNums = 4;
        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new NewThreadWithIndex("NettyClientPublicExecutor_"));

        this.scanExecutor = new ThreadPoolExecutor(4, 10, 60, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(32), new NewThreadWithIndex("NettyClientScan_thread_")
        );

        if (eventLoopGroup != null) {
            this.eventLoopGroupWorker = eventLoopGroup;
        } else {
            this.eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
                private final AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("NettyClientSelector_%d", this.threadIndex.incrementAndGet()));
                }
            });
        }
        this.defaultEventExecutorGroup = eventExecutorGroup;

    }

    private static int initValueIndex() {
        Random r = new Random();
        return r.nextInt(999);
    }

    @Override
    public void start() {
        if (this.defaultEventExecutorGroup == null) {
            this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
                    4,
                    new ThreadFactory() {
                        private final AtomicInteger threadIndex = new AtomicInteger(0);

                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
                        }
                    });
        }
        Bootstrap handler = this.bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        ch.pipeline().addLast(
                                defaultEventExecutorGroup,
                                new NettyEncoder(),
                                new NettyDecoder(),
                                new IdleStateHandler(0, 0, 120),
                                new NettyConnectManageHandler(),
                                new NettyClientHandler());
                    }
                });
        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    NettyRemotingClient.this.scanResponseTable();
                } catch (Throwable e) {
                }
            }
        }, 1000 * 3, 1000);

//        this.timer.scheduleAtFixedRate(new TimerTask() {
//            @Override
//            public void run() {
//                try {
//                    NettyRemotingClient.this.scanChannelTablesOfNameServer();
//                } catch (Exception e) {
//                    LOGGER.error("scanChannelTablesOfNameServer exception", e);
//                }
//            }
//        }, 1000 * 3, 10 * 1000);

        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    NettyRemotingClient.this.scanAvailableNameSrv();
                } catch (Exception e) {
                }
            }
        }, 0, 3000);

    }


    private Bootstrap createBootstrap() {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(this.eventLoopGroupWorker).channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline pipeline = ch.pipeline();


                        pipeline.addLast(
                                defaultEventExecutorGroup,
                                new NettyEncoder(),
                                new NettyDecoder(),
                                new IdleStateHandler(0, 0, 120),
                                new NettyConnectManageHandler(),
                                new NettyClientHandler());
                    }
                });

        return bootstrap;
    }

    // Do not use RemotingUtil, it will directly resolve the domain
    private String[] getHostAndPort(String address) {
        return address.split(":");
    }

    @Override
    public void shutdown() {
        try {
            this.timer.cancel();

            for (String addr : this.channelTables.keySet()) {
                this.closeChannel(addr, this.channelTables.get(addr).getChannel());
            }

            this.channelTables.clear();

            this.eventLoopGroupWorker.shutdownGracefully();

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        } catch (Exception e) {
            LOGGER.error("NettyRemotingClient shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            } catch (Exception e) {
                LOGGER.error("NettyRemotingServer shutdown exception, ", e);
            }
        }

        if (this.scanExecutor != null) {
            try {
                this.scanExecutor.shutdown();
            } catch (Exception e) {
                LOGGER.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }

    public void closeChannel(final String addr, final Channel channel) {
        if (null == channel) {
            return;
        }

        final String addrRemote = null == addr ? RemotingHelper.parseChannelRemoteAddr(channel) : addr;

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    final ChannelWrapper prevCW = this.channelTables.get(addrRemote);

                    LOGGER.info("closeChannel: begin close the channel[{}] Found: {}", addrRemote, prevCW != null);

                    if (null == prevCW) {
                        LOGGER.info("closeChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    } else if (prevCW.getChannel() != channel) {
                        LOGGER.info("closeChannel: the channel[{}] has been closed before, and has been created again, nothing to do.",
                                addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        LOGGER.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                    }

                    RemotingUtil.closeChannel(channel);
                } catch (Exception e) {
                    LOGGER.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                LOGGER.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            LOGGER.error("closeChannel exception", e);
        }
    }

    public void closeChannel(final Channel channel) {
        if (null == channel) {
            return;
        }

        try {
            if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    boolean removeItemFromTable = true;
                    ChannelWrapper prevCW = null;
                    String addrRemote = null;
                    for (Map.Entry<String, ChannelWrapper> entry : channelTables.entrySet()) {
                        String key = entry.getKey();
                        ChannelWrapper prev = entry.getValue();
                        if (prev.getChannel() != null) {
                            if (prev.getChannel() == channel) {
                                prevCW = prev;
                                addrRemote = key;
                                break;
                            }
                        }
                    }

                    if (null == prevCW) {
                        LOGGER.info("eventCloseChannel: the channel[{}] has been removed from the channel table before", addrRemote);
                        removeItemFromTable = false;
                    }

                    if (removeItemFromTable) {
                        this.channelTables.remove(addrRemote);
                        LOGGER.info("closeChannel: the channel[{}] was removed from channel table", addrRemote);
                        RemotingUtil.closeChannel(channel);
                    }
                } catch (Exception e) {
                    LOGGER.error("closeChannel: close the channel exception", e);
                } finally {
                    this.lockChannelTables.unlock();
                }
            } else {
                LOGGER.warn("closeChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
            }
        } catch (InterruptedException e) {
            LOGGER.error("closeChannel exception", e);
        }
    }

    @Override
    public void updateNameServerAddressList(List<String> addrs) {
        List<String> old = this.namesrvAddrList.get();
        boolean update = false;

        if (!addrs.isEmpty()) {
            if (null == old) {
                update = true;
            } else if (addrs.size() != old.size()) {
                update = true;
            } else {
                for (int i = 0; i < addrs.size() && !update; i++) {
                    if (!old.contains(addrs.get(i))) {
                        update = true;
                    }
                }
            }

            if (update) {
                Collections.shuffle(addrs);
                LOGGER.info("name server address updated. NEW : {} , OLD: {}", addrs, old);
                this.namesrvAddrList.set(addrs);

                // should close the channel if choosed addr is not exist.
                if (this.namesrvAddrChoosed.get() != null && !addrs.contains(this.namesrvAddrChoosed.get())) {
                    String namesrvAddr = this.namesrvAddrChoosed.get();
                    for (String addr : this.channelTables.keySet()) {
                        if (addr.contains(namesrvAddr)) {
                            ChannelWrapper channelWrapper = this.channelTables.get(addr);
                            if (channelWrapper != null) {
                                closeChannel(channelWrapper.getChannel());
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public RemotingCommand invokeSync(String addr, final RemotingCommand request, long timeoutMillis)
            throws Exception {
        long beginStartTime = System.currentTimeMillis();
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                doBeforeRpcHooks(addr, request);
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    throw new Exception("invokeSync call the addr[" + addr + "] timeout");
                }
                RemotingCommand response = this.invokeSyncImpl(channel, request, timeoutMillis - costTime);
                doAfterRpcHooks(RemotingHelper.parseChannelRemoteAddr(channel), request, response);
                this.updateChannelLastResponseTime(addr);
                return response;
            } catch (Exception e) {
                LOGGER.warn("invokeSync: close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new Exception(addr);
        }
    }

    @Override
    public void closeChannels(List<String> addrList) {
        for (String addr : addrList) {
            ChannelWrapper cw = this.channelTables.get(addr);
            if (cw == null) {
                continue;
            }
            this.closeChannel(addr, cw.getChannel());
        }
        interruptPullRequests(new HashSet<>(addrList));
    }

    private void interruptPullRequests(Set<String> brokerAddrSet) {
        for (ResponseFuture responseFuture : responseTable.values()) {
            RemotingCommand cmd = responseFuture.getRequest();
            if (cmd == null) {
                continue;
            }
            String remoteAddr = RemotingHelper.parseChannelRemoteAddr(responseFuture.getChannel());
            // interrupt only pull message request
            if (brokerAddrSet.contains(remoteAddr) && (cmd.getCode() == 11 || cmd.getCode() == 361)) {
                LOGGER.info("interrupt {}", cmd);
                responseFuture.interrupt();
            }
        }
    }

    private void updateChannelLastResponseTime(final String addr) {
        String address = addr;
        if (address == null) {
            address = this.namesrvAddrChoosed.get();
        }
        if (address == null) {
            LOGGER.warn("[updateChannelLastResponseTime] could not find address!!");
            return;
        }
        ChannelWrapper channelWrapper = this.channelTables.get(address);
        if (channelWrapper != null && channelWrapper.isOK()) {
            channelWrapper.updateLastResponseTime();
        }
    }

    private Channel getAndCreateChannel(final String addr) throws InterruptedException {
        if (null == addr) {
            return getAndCreateNameserverChannel();
        }

        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }

        return this.createChannel(addr);
    }

    private Channel getAndCreateNameserverChannel() throws InterruptedException {
        String addr = this.namesrvAddrChoosed.get();
        if (addr != null) {
            ChannelWrapper cw = this.channelTables.get(addr);
            if (cw != null && cw.isOK()) {
                return cw.getChannel();
            }
        }

        final List<String> addrList = this.namesrvAddrList.get();
        if (this.namesrvChannelLock.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                addr = this.namesrvAddrChoosed.get();
                if (addr != null) {
                    ChannelWrapper cw = this.channelTables.get(addr);
                    if (cw != null && cw.isOK()) {
                        return cw.getChannel();
                    }
                }

                if (addrList != null && !addrList.isEmpty()) {
                    for (int i = 0; i < addrList.size(); i++) {
                        int index = this.namesrvIndex.incrementAndGet();
                        index = Math.abs(index);
                        index = index % addrList.size();
                        String newAddr = addrList.get(index);

                        this.namesrvAddrChoosed.set(newAddr);
                        LOGGER.info("new name server is chosen. OLD: {} , NEW: {}. namesrvIndex = {}", addr, newAddr, namesrvIndex);
                        Channel channelNew = this.createChannel(newAddr);
                        if (channelNew != null) {
                            return channelNew;
                        }
                    }
                    throw new Exception(addrList.toString());
                }
            } catch (Exception e) {
                LOGGER.error("getAndCreateNameserverChannel: create name server channel exception", e);
            } finally {
                this.namesrvChannelLock.unlock();
            }
        } else {
            LOGGER.warn("getAndCreateNameserverChannel: try to lock name server, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        return null;
    }

    private Channel createChannel(final String addr) throws InterruptedException {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.getChannel();
        }

        if (this.lockChannelTables.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
            try {
                cw = this.channelTables.get(addr);
                if (cw.isOK()) {
                    return cw.getChannel();
                } else {
                    this.channelTables.remove(addr);
                }

            } catch (Exception e) {
                LOGGER.error("createChannel: create channel exception", e);
            } finally {
                this.lockChannelTables.unlock();
            }
        } else {
            LOGGER.warn("createChannel: try to lock channel table, but timeout, {}ms", LOCK_TIMEOUT_MILLIS);
        }

        if (cw != null) {
            ChannelFuture channelFuture = cw.getChannelFuture();
            if (channelFuture.awaitUninterruptibly(3000)) {
                if (cw.isOK()) {
                    LOGGER.info("createChannel: connect remote host[{}] success, {}", addr, channelFuture.toString());
                    return cw.getChannel();
                } else {
                    LOGGER.warn("createChannel: connect remote host[" + addr + "] failed, " + channelFuture.toString());
                }
            } else {
                LOGGER.warn("createChannel: connect remote host[{}] timeout {}ms, {}", addr, 3000,
                        channelFuture.toString());
            }
        }

        return null;
    }

    @Override
    public void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
            throws Exception {
        long beginStartTime = System.currentTimeMillis();
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                doBeforeRpcHooks(addr, request);
                long costTime = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTime) {
                    throw new Exception("invokeAsync call the addr[" + addr + "] timeout");
                }
                this.invokeAsyncImpl(channel, request, timeoutMillis - costTime, new InvokeCallbackWrapper(invokeCallback, addr));
            } catch (Exception e) {
                LOGGER.warn("invokeAsync: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new Exception(addr);
        }
    }

    @Override
    public void invokeOneway(String addr, RemotingCommand request, long timeoutMillis) throws Exception {
        final Channel channel = this.getAndCreateChannel(addr);
        if (channel != null && channel.isActive()) {
            try {
                doBeforeRpcHooks(addr, request);
                this.invokeOnewayImpl(channel, request, timeoutMillis);
            } catch (Exception e) {
                LOGGER.warn("invokeOneway: send request exception, so close the channel[{}]", addr);
                this.closeChannel(addr, channel);
                throw e;
            }
        } else {
            this.closeChannel(addr, channel);
            throw new Exception(addr);
        }
    }

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
    public boolean isChannelWritable(String addr) {
        ChannelWrapper cw = this.channelTables.get(addr);
        if (cw != null && cw.isOK()) {
            return cw.isWritable();
        }
        return true;
    }

    @Override
    public List<String> getNameServerAddressList() {
        return this.namesrvAddrList.get();
    }

    @Override
    public List<String> getAvailableNameSrvList() {
        return new ArrayList<>(this.availableNamesrvAddrMap.keySet());
    }

    @Override
    public ExecutorService getCallbackExecutor() {

        return callbackExecutor != null ? callbackExecutor : publicExecutor;
    }

    @Override
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.callbackExecutor = callbackExecutor;
    }

    protected void scanChannelTablesOfNameServer() {
        List<String> nameServerList = this.namesrvAddrList.get();
        if (nameServerList == null) {
            LOGGER.warn("[SCAN] Addresses of name server is empty!");
            return;
        }

        for (String addr : this.channelTables.keySet()) {
            ChannelWrapper channelWrapper = this.channelTables.get(addr);
            if (channelWrapper == null) {
                continue;
            }

            if ((System.currentTimeMillis() - channelWrapper.getLastResponseTime()) > 1000 * 60) {
                LOGGER.warn("[SCAN] No response after {} from name server {}, so close it!", channelWrapper.getLastResponseTime(),
                        addr);
                closeChannel(addr, channelWrapper.getChannel());
            }
        }
    }

    private void scanAvailableNameSrv() {
        List<String> nameServerList = this.namesrvAddrList.get();
        if (nameServerList == null) {
            LOGGER.debug("scanAvailableNameSrv Addresses of name server is empty!");
            return;
        }

        for (final String namesrvAddr : nameServerList) {
            scanExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Channel channel = NettyRemotingClient.this.getAndCreateChannel(namesrvAddr);
                        if (channel != null) {
                            NettyRemotingClient.this.availableNamesrvAddrMap.putIfAbsent(namesrvAddr, true);
                        } else {
                            NettyRemotingClient.this.availableNamesrvAddrMap.remove(namesrvAddr);
                        }
                    } catch (Exception e) {
                        LOGGER.error("scanAvailableNameSrv get channel of {} failed, ", namesrvAddr, e);
                    }
                }
            });
        }

    }

    static class ChannelWrapper {
        private final ChannelFuture channelFuture;
        // only affected by sync or async request, oneway is not included.
        private long lastResponseTime;

        public ChannelWrapper(ChannelFuture channelFuture) {
            this.channelFuture = channelFuture;
            this.lastResponseTime = System.currentTimeMillis();
        }

        public boolean isOK() {
            return this.channelFuture.channel() != null && this.channelFuture.channel().isActive();
        }

        public boolean isWritable() {
            return this.channelFuture.channel().isWritable();
        }

        private Channel getChannel() {
            return this.channelFuture.channel();
        }

        public ChannelFuture getChannelFuture() {
            return channelFuture;
        }

        public long getLastResponseTime() {
            return this.lastResponseTime;
        }

        public void updateLastResponseTime() {
            this.lastResponseTime = System.currentTimeMillis();
        }
    }

    class InvokeCallbackWrapper implements InvokeCallback {

        private final InvokeCallback invokeCallback;
        private final String addr;

        public InvokeCallbackWrapper(InvokeCallback invokeCallback, String addr) {
            this.invokeCallback = invokeCallback;
            this.addr = addr;
        }

        @Override
        public void operationComplete(ResponseFuture responseFuture) {
            if (responseFuture != null && responseFuture.isSendRequestOK() && responseFuture.getResponseCommand() != null) {
                NettyRemotingClient.this.updateChannelLastResponseTime(addr);
            }
            this.invokeCallback.operationComplete(responseFuture);
        }
    }

    class NettyClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            processMessageReceived(ctx, msg);
        }
    }

    class NettyConnectManageHandler extends ChannelDuplexHandler {
        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
                            ChannelPromise promise) throws Exception {
            final String local = localAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(localAddress);
            final String remote = remoteAddress == null ? "UNKNOWN" : RemotingHelper.parseSocketAddressAddr(remoteAddress);
            LOGGER.info("NETTY CLIENT PIPELINE: CONNECT  {} => {}", local, remote);

            super.connect(ctx, remoteAddress, localAddress, promise);

        }

        @Override
        public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            LOGGER.info("NETTY CLIENT PIPELINE: DISCONNECT {}", remoteAddress);
            closeChannel(ctx.channel());
            super.disconnect(ctx, promise);

        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            LOGGER.info("NETTY CLIENT PIPELINE: CLOSE {}", remoteAddress);
            closeChannel(ctx.channel());
            super.close(ctx, promise);
            NettyRemotingClient.this.failFast(ctx.channel());

        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof IdleStateEvent) {
                IdleStateEvent event = (IdleStateEvent) evt;
                if (event.state().equals(IdleState.ALL_IDLE)) {
                    final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    LOGGER.warn("NETTY CLIENT PIPELINE: IDLE exception [{}]", remoteAddress);
                    closeChannel(ctx.channel());
                }
            }

            ctx.fireUserEventTriggered(evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
            LOGGER.warn("NETTY CLIENT PIPELINE: exceptionCaught {}", remoteAddress);
            LOGGER.warn("NETTY CLIENT PIPELINE: exceptionCaught exception.", cause);
            closeChannel(ctx.channel());
        }
    }
}
