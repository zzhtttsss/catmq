package org.catmq.pipline.processor;

import lombok.extern.slf4j.Slf4j;
import org.catmq.common.MessageEntry;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.SendMessage2StorerRequest;
import org.catmq.protocol.service.SendMessage2StorerResponse;
import org.catmq.storer.Storer;

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
        long segmentId = ctx.getSegmentId();
        long entryId = ctx.getEntryId();
        MessageEntry messageEntry = new MessageEntry(segmentId, entryId, request.getBody().toByteArray());
        storer.getSegmentStorage().appendEntry2WriteCache(messageEntry);
        storer.getFlushMessageEntryService().putMessageEntry2Queue(messageEntry);
//        try {
//            messageEntry.getWaiter().await();
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
        SendMessage2StorerResponse response = SendMessage2StorerResponse.newBuilder().setAck(true).setRes("Success").build();
        return response;
    }


}
