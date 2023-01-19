package org.catmq.context;

import java.util.HashMap;
import java.util.Map;

public class RequestContext {
    public static final String INNER_ACTION_PREFIX = "Inner";

    private final Map<String, Object> value = new HashMap<>();


    public static RequestContext create() {
        return new RequestContext();
    }

    public static RequestContext createForInner(String actionName) {
        return create().setAction(INNER_ACTION_PREFIX + actionName);
    }

    public static RequestContext createForInner(Class<?> clazz) {
        return createForInner(clazz.getSimpleName());
    }

    public RequestContext withVal(String key, Object val) {
        this.value.put(key, val);
        return this;
    }

    public <T> T getVal(String key) {
        return (T) this.value.get(key);
    }

    /**
     * Wrap broker path on zk into ctx.
     *
     * @param brokerPath broker path brokerPath
     * @return this
     */
    public RequestContext setBrokerPath(String brokerPath) {
        return withVal(ContextVariable.BROKER_PATH, brokerPath);
    }

    public String getBrokerPath() {
        return this.getVal(ContextVariable.BROKER_PATH);
    }

    public RequestContext setLocalAddress(String localAddress) {
        this.withVal(ContextVariable.LOCAL_ADDRESS, localAddress);
        return this;
    }

    public String getLocalAddress() {
        return this.getVal(ContextVariable.LOCAL_ADDRESS);
    }

    public RequestContext setRemoteAddress(String remoteAddress) {
        this.withVal(ContextVariable.REMOTE_ADDRESS, remoteAddress);
        return this;
    }

    public String getRemoteAddress() {
        return this.getVal(ContextVariable.REMOTE_ADDRESS);
    }

    public RequestContext setProducerId(Long producerId) {
        return withVal(ContextVariable.PRODUCER_ID, producerId);
    }

    public Long getProducerId() {
        return this.getVal(ContextVariable.PRODUCER_ID);
    }

    public RequestContext setConsumerId(Long consumerId) {
        return withVal(ContextVariable.CONSUMER_ID, consumerId);
    }

    public Long getConsumerId() {
        return this.getVal(ContextVariable.CONSUMER_ID);
    }

    public RequestContext setAction(String action) {
        this.withVal(ContextVariable.ACTION, action);
        return this;
    }

    public String getAction() {
        return this.getVal(ContextVariable.ACTION);
    }

    public void print() {
        this.value.forEach((n, v) -> {
            System.out.println(n + " : " + v);
        });
    }
}
