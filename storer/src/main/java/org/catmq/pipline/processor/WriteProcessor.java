package org.catmq.pipline.processor;

import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.*;
import org.catmq.storer.Storer;

public class WriteProcessor implements Processor<SendMessage2StorerRequest, SendMessage2StorerResponse> {
    @Override
    public SendMessage2StorerResponse process(RequestContext ctx, SendMessage2StorerRequest request) {
        Storer storer = Storer.STORER;




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
