package org.catmq.remoting;

import org.catmq.remoting.netty.NettyRequestProcessor;
import org.catmq.remoting.protocol.RemotingCommand;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * @author BYL
 */
public interface RemotingClient extends RemotingService {

    RemotingCommand invokeSync(final String addr, final RemotingCommand request,
                               final long timeoutMillis) throws Exception;

    void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis,
                     final InvokeCallback invokeCallback) throws Exception;

    void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis)
            throws Exception;

    void registerProcessor(final int requestCode, final NettyRequestProcessor processor,
                           final ExecutorService executor);

    void setCallbackExecutor(final ExecutorService callbackExecutor);

    void closeChannels(final List<String> addrList);
}
