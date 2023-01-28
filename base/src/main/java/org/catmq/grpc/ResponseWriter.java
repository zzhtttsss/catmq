package org.catmq.grpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
public class ResponseWriter {
    private static final Logger logger = LoggerFactory.getLogger(ResponseWriter.class);

    public enum ResponseWriterEnum {
        INSTANCE;
        private final ResponseWriter responseWriter;

        ResponseWriterEnum() {
            responseWriter = new ResponseWriter();
        }

        public ResponseWriter getInstance() {
            return responseWriter;
        }
    }

    public <T> void write(StreamObserver<T> observer, final T response) {
        if (writeResponse(observer, response)) {
            observer.onCompleted();
        }
    }

    public <T> boolean writeResponse(StreamObserver<T> observer, final T response) {
        if (null == response) {
            return false;
        }
        if (isCancelled(observer)) {
            logger.warn("client has cancelled the request. response to write: {}", response);
            return false;
        }
        try {
            observer.onNext(response);
        } catch (StatusRuntimeException statusRuntimeException) {
            if (Status.CANCELLED.equals(statusRuntimeException.getStatus())) {
                logger.warn("client has cancelled the request. response to write: {}", response);
                return false;
            }
            throw statusRuntimeException;
        }
        return true;
    }

    public <T> boolean isCancelled(StreamObserver<T> observer) {
        if (observer instanceof final ServerCallStreamObserver<T> serverCallStreamObserver) {
            return serverCallStreamObserver.isCancelled();
        }
        return false;
    }
}
