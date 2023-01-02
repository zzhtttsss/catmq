package org.catmq.grpc;

import org.catmq.constant.ClientErrorCode;
import org.catmq.constant.ResponseCode;
import org.catmq.protocol.definition.Code;
import org.catmq.protocol.definition.Status;
import org.catmq.util.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ResponseBuilder {

    private static final Logger log = LoggerFactory.getLogger(ResponseBuilder.class);
    protected static final Map<Integer, Code> RESPONSE_CODE_MAPPING = new ConcurrentHashMap<>();

    static {
        RESPONSE_CODE_MAPPING.put(ResponseCode.SUCCESS, Code.OK);
        RESPONSE_CODE_MAPPING.put(ResponseCode.SYSTEM_BUSY, Code.TOO_MANY_REQUESTS);
        RESPONSE_CODE_MAPPING.put(ResponseCode.REQUEST_CODE_NOT_SUPPORTED, Code.NOT_IMPLEMENTED);
        RESPONSE_CODE_MAPPING.put(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST, Code.CONSUMER_GROUP_NOT_FOUND);
        RESPONSE_CODE_MAPPING.put(ClientErrorCode.ACCESS_BROKER_TIMEOUT, Code.PROXY_TIMEOUT);
    }

    public enum ResponseBuilderEnum {
        INSTANCE;
        private ResponseBuilder responseBuilder;
        ResponseBuilderEnum() {
            responseBuilder = new ResponseBuilder();
        }
        public ResponseBuilder getInstance() {
            return responseBuilder;
        }
    }

    public Status buildStatus(Throwable t) {
        t = ExceptionUtil.getRealException(t);

        if (t instanceof NullPointerException) {
            return buildStatus(Code.TOPIC_NOT_FOUND, t.getMessage());

        }
        log.error("internal server error", t);
        return buildStatus(Code.INTERNAL_SERVER_ERROR, ExceptionUtil.getErrorDetailMessage(t));
    }

    public Status buildStatus(Code code, String message) {
        return Status.newBuilder()
                .setCode(code)
                .setMessage(message)
                .build();
    }

    public Status buildStatus(int remotingResponseCode, String remark) {
        String message = remark;
        if (message == null) {
            message = String.valueOf(remotingResponseCode);
        }
        return Status.newBuilder()
                .setCode(buildCode(remotingResponseCode))
                .setMessage(message)
                .build();
    }

    public Code buildCode(int remotingResponseCode) {
        return RESPONSE_CODE_MAPPING.getOrDefault(remotingResponseCode, Code.INTERNAL_SERVER_ERROR);
    }
}
