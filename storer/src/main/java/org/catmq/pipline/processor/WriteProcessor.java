package org.catmq.pipline.processor;

import lombok.extern.slf4j.Slf4j;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.*;
import org.catmq.storage.messageLog.MessageEntry;
import org.catmq.storer.Storer;

import java.util.concurrent.CompletableFuture;

@Slf4j
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
        try {
            messageEntry.getWaiter().await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        SendMessage2StorerResponse response = SendMessage2StorerResponse.newBuilder().build();
        return response;
    }


}
