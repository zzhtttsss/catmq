package org.catmq.pipline.processor;

import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.*;
import org.catmq.storage.messageLog.MessageEntry;
import org.catmq.storer.Storer;

public class WriteProcessor implements Processor<SendMessage2StorerRequest, SendMessage2StorerResponse> {

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

    @Override
    public SendMessage2StorerResponse process(RequestContext ctx, SendMessage2StorerRequest request) {
        Storer storer = Storer.STORER;
        MessageEntry messageEntry = new MessageEntry(request.getMsgId(), ctx.getChunkId(), request.getBody().toByteArray());
        storer.flushMessageEntryService.putMessageLogEntry2Queue(messageEntry);



        return null;
    }


}
