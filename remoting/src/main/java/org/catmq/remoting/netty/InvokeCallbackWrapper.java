package org.catmq.remoting.netty;

import lombok.AllArgsConstructor;
import org.catmq.remoting.InvokeCallback;

/**
 * @author BYL
 */
@AllArgsConstructor
public class InvokeCallbackWrapper implements InvokeCallback {

    private final InvokeCallback invokeCallback;
    private final String addr;

    @Override
    public void operationComplete(ResponseFuture responseFuture) {
        this.invokeCallback.operationComplete(responseFuture);
    }
}
