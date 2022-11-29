package org.catmq.remoting.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.catmq.remoting.InvokeCallback;
import org.catmq.remoting.RPCHook;
import org.catmq.remoting.common.Pair;
import org.catmq.remoting.common.RemotingHelper;
import org.catmq.remoting.protocol.RemotingCommand;
import org.catmq.remoting.protocol.RemotingSysResponseCode;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Logger;

public abstract class AbstractNettyRemoting {

    /**
     * Remoting logger instance.
     */
    private static final Logger log = Logger.getLogger(AbstractNettyRemoting.class.getCanonicalName());

    /**
     * This map caches all ongoing requests whose key is requestId number
     */
    protected final ConcurrentMap<Integer, ResponseFuture> responseTable =
            new ConcurrentHashMap<>(256);

    /**
     * This container holds all processors per request code for each incoming request, we may look up the
     * responding processor in this map to handle the request.
     */
    protected final HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>> processorTable =
            new HashMap<>(64);


    /**
     * The default request processor to use in case there is no exact match in {@link #processorTable} per request code.
     */
    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessorPair;


    /**
     * custom rpc hooks
     */
    protected List<RPCHook> rpcHooks = new ArrayList<>();


    /**
     * Entry of incoming command processing.
     *
     * <p>
     * <strong>Note:</strong>
     * The incoming remoting command may be
     * <ul>
     * <li>An inquiry request from a remote peer component;</li>
     * <li>A response to a previous request issued by this very participant.</li>
     * </ul>
     * </p>
     *
     * @param ctx Channel handler context.
     * @param msg incoming remoting command.
     */
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) {
        if (msg != null) {
            switch (msg.getType()) {
                case REQUEST_COMMAND:
                    processRequestCommand(ctx, msg);
                    break;
                case RESPONSE_COMMAND:
                    processResponseCommand(ctx, msg);
                    break;
                default:
                    log.warning("Error msg type");
                    break;
            }
        }
    }

    protected void doBeforeRpcHooks(String addr, RemotingCommand request) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook : rpcHooks) {
                rpcHook.doBeforeRequest(addr, request);
            }
        }
    }

    public void doAfterRpcHooks(String addr, RemotingCommand request, RemotingCommand response) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook : rpcHooks) {
                rpcHook.doAfterResponse(addr, request, response);
            }
        }
    }

    /**
     * Process incoming request command issued by remote peer.
     *
     * @param ctx channel handler context.
     * @param cmd request command.
     */
    public synchronized void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        final Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessorPair : matched;
        final int requestId = cmd.getRequestId();

        if (pair == null) {
            if (cmd.isOnewayRPC()) {
                return;
            }
            String error = " request type " + cmd.getCode() + " not supported";
            final RemotingCommand response =
                    RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            response.setRequestId(requestId);
            ctx.writeAndFlush(response);
            return;
        }

        Runnable run = buildProcessRequestHandler(ctx, cmd, pair, requestId);

        if (pair.getObject1().rejectRequest()) {
            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                    "[REJECTREQUEST]system busy, start flow control for a while");
            response.setRequestId(requestId);
            ctx.writeAndFlush(response);
            return;
        }

        try {
            final RequestTask requestTask = new RequestTask(run, ctx.channel(), cmd);
            //async execute task, current thread return directly
            pair.getObject2().submit(requestTask);
        } catch (RejectedExecutionException e) {
            if ((System.currentTimeMillis() % 10000) == 0) {
                log.warning(RemotingHelper.parseChannelRemoteAddr(ctx.channel())
                        + ", too many requests and system thread pool busy, RejectedExecutionException "
                        + pair.getObject2().toString()
                        + " request code: " + cmd.getCode());
            }

            if (!cmd.isOnewayRPC()) {
                final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                        "[OVERLOAD]system busy, start flow control for a while");
                response.setRequestId(requestId);
                ctx.writeAndFlush(response);
            }
        }
    }

    private Runnable buildProcessRequestHandler(ChannelHandlerContext ctx, RemotingCommand cmd, Pair<NettyRequestProcessor, ExecutorService> pair, int requestId) {
        return () -> {
            Exception exception = null;
            RemotingCommand response;

            try {
                String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                try {
                    doBeforeRpcHooks(remoteAddr, cmd);
                } catch (Exception e) {
                    exception = e;
                }

                if (exception == null) {
                    response = pair.getObject1().processRequest(ctx, cmd);
                } else {
                    response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, null);
                }

                try {
                    doAfterRpcHooks(remoteAddr, cmd, response);
                } catch (Exception e) {
                    exception = e;
                }

                if (exception != null) {
                    throw exception;
                }

                if (!cmd.isOnewayRPC()) {
                    if (response != null) {
                        response.setRequestId(requestId);
                        response.markResponseType();
                        try {
                            ctx.writeAndFlush(response);
                        } catch (Throwable e) {
                            log.warning("process request over, but response failed " + e);
                            log.warning(cmd.toString());
                            log.warning(response.toString());
                        }
                    }
                }
            } catch (Throwable e) {
                log.warning("process request exception " + e);
                log.warning(cmd.toString());

                if (!cmd.isOnewayRPC()) {
                    response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                            "none one way error");
                    response.setRequestId(requestId);
                    ctx.writeAndFlush(response);
                }
            }
        };
    }

    /**
     * Process response from remote peer to the previous issued requests.
     *
     * @param ctx channel handler context.
     * @param cmd response command instance.
     */
    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        final int requestId = cmd.getRequestId();
        final ResponseFuture responseFuture = responseTable.get(requestId);
        if (responseFuture != null) {
            responseFuture.setResponseCommand(cmd);

            responseTable.remove(requestId);

            if (responseFuture.getInvokeCallback() != null) {
                executeInvokeCallback(responseFuture);
            } else {
                responseFuture.putResponse(cmd);
            }
        } else {
            log.warning("receive response, but not matched any request, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            log.warning(cmd.toString());
        }
    }

    /**
     * Execute callback in callback executor. If callback executor is null, run directly in current thread
     */
    private void executeInvokeCallback(final ResponseFuture responseFuture) {
        boolean runInThisThread = false;
        ExecutorService executor = this.getCallbackExecutor();
        if (executor != null && !executor.isShutdown()) {
            try {
                executor.submit(() -> {
                    try {
                        responseFuture.executeInvokeCallback();
                    } catch (Throwable e) {
                        log.warning("execute callback in executor exception, and callback throw " + e);
                    }
                });
            } catch (Exception e) {
                runInThisThread = true;
                log.warning("execute callback in executor exception, maybe executor busy " + e);
            }
        } else {
            runInThisThread = true;
        }

        if (runInThisThread) {
            try {
                responseFuture.executeInvokeCallback();
            } catch (Throwable e) {
                log.warning("executeInvokeCallback Exception " + e);
            }
        }
    }

    /**
     * Custom RPC hooks.
     *
     * @return RPC hooks if specified; null otherwise.
     */
    public List<RPCHook> getRPCHook() {
        return rpcHooks;
    }

    public void registerRPCHook(RPCHook rpcHook) {
        if (rpcHook != null && !rpcHooks.contains(rpcHook)) {
            rpcHooks.add(rpcHook);
        }
    }

    public void clearRPCHook() {
        rpcHooks.clear();
    }

    /**
     * This method specifies thread pool to use while invoking callback methods.
     *
     * @return Dedicated thread pool instance if specified; or null if the callback is supposed to be executed in the
     * netty event-loop thread.
     */
    public abstract ExecutorService getCallbackExecutor();

    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request,
                                          final long timeoutMillis)
            throws Exception {
        //get the request id
        final int requestId = request.getRequestId();

        try {
            final ResponseFuture responseFuture = new ResponseFuture(channel, requestId, timeoutMillis, null);
            this.responseTable.put(requestId, responseFuture);
            final SocketAddress addr = channel.remoteAddress();
            channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
                if (f.isSuccess()) {
                    responseFuture.setSendRequestOK(true);
                    return;
                }
                responseFuture.setSendRequestOK(false);
                responseTable.remove(requestId);
                responseFuture.setCause(f.cause());
                responseFuture.putResponse(null);
                log.warning(String.format("Failed to write a request command to %s, caused by underlying I/O operation failure\n", addr));
            });

            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
            if (responseCommand == null) {
                throw new Exception("Not receiving response from " + RemotingHelper.parseSocketAddressAddr(addr));
            }

            return responseCommand;
        } finally {
            this.responseTable.remove(requestId);
        }
    }

    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis,
                                final InvokeCallback invokeCallback) throws Exception {
        long beginStartTime = System.currentTimeMillis();
        final int requestId = request.getRequestId();
        long costTime = System.currentTimeMillis() - beginStartTime;
        if (timeoutMillis < costTime) {
            throw new Exception("invokeAsyncImpl call timeout");
        }

        final ResponseFuture responseFuture = new ResponseFuture(channel, requestId, timeoutMillis - costTime, invokeCallback);
        this.responseTable.put(requestId, responseFuture);
        try {
            channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
                if (f.isSuccess()) {
                    responseFuture.setSendRequestOK(true);
                    return;
                }
                requestFail(requestId);
                log.warning(String.format("send a request command to channel <%s> failed.", RemotingHelper.parseChannelRemoteAddr(channel)));
            });
        } catch (Exception e) {
            log.warning("send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel) + "> Exception " + e);
        }

    }

    private void requestFail(final int requestId) {
        ResponseFuture responseFuture = responseTable.remove(requestId);
        if (responseFuture != null) {
            responseFuture.setSendRequestOK(false);
            responseFuture.putResponse(null);
            try {
                executeInvokeCallback(responseFuture);
            } catch (Throwable e) {
                log.warning("execute callback in requestFail, and callback throw " + e);
            }
        }
    }

    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis) {
        request.markOnewayRPC();
        try {
            channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
                if (!f.isSuccess()) {
                    log.warning("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                }
            });
        } catch (Exception e) {
            log.warning("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
        }

    }

    /**
     * Scan the responseTable to delete expired requests
     */
    public void scanResponseTable() {
        final List<ResponseFuture> rfList = new LinkedList<>();
        Iterator<Map.Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();
            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis()) <= System.currentTimeMillis()) {
                it.remove();
                rfList.add(rep);
            }
        }
        for (ResponseFuture rf : rfList) {
            try {
                executeInvokeCallback(rf);
            } catch (Throwable e) {
                log.warning("scanResponseTable, operationComplete Exception " + e);
            }
        }
    }
}
