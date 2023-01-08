package org.catmq.grpc;

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

    public Map<String, Object> getValue() {
        return this.value;
    }

    public RequestContext withVal(String key, Object val) {
        this.value.put(key, val);
        return this;
    }

    public <T> T getVal(String key) {
        return (T) this.value.get(key);
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

    public RequestContext setClientID(String clientID) {
        this.withVal(ContextVariable.CLIENT_ID, clientID);
        return this;
    }

    public String getClientID() {
        return this.getVal(ContextVariable.CLIENT_ID);
    }

    public RequestContext setLanguage(String language) {
        this.withVal(ContextVariable.LANGUAGE, language);
        return this;
    }

    public String getLanguage() {
        return this.getVal(ContextVariable.LANGUAGE);
    }

    public RequestContext setClientVersion(String clientVersion) {
        this.withVal(ContextVariable.CLIENT_VERSION, clientVersion);
        return this;
    }

    public String getClientVersion() {
        return this.getVal(ContextVariable.CLIENT_VERSION);
    }

    public RequestContext setAction(String action) {
        this.withVal(ContextVariable.ACTION, action);
        return this;
    }
    public String getAction() {
        return this.getVal(ContextVariable.ACTION);
    }

    public long getChunkId() {
        return Long.parseLong(this.getVal(ContextVariable.CHUNK_ID));
    }

    public RequestContext setChunkId(String chunkId) {
        this.withVal(ContextVariable.CHUNK_ID, chunkId);
        return this;
    }

}
