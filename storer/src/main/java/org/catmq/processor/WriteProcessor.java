package org.catmq.processor;

import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.*;

public class WriteProcessor implements Processor<SendMessage2StorerRequest, SendMessage2StorerResponse> {
    @Override
    public SendMessage2StorerResponse process(RequestContext ctx, SendMessage2StorerRequest request) {



        return null;
    }

    public enum WriteProcessorEnum {
        INSTANCE;
        private final WriteProcessor writeProcessor;
        WriteProcessorEnum() {
            writeProcessor = new WriteProcessor();
        }
        public WriteProcessor getInstance() {
            return writeProcessor;
        }
    }
}
