package org.catmq.pipline.processor;

import lombok.extern.slf4j.Slf4j;
import org.catmq.entity.FlushMode;
import org.catmq.common.MessageEntry;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.SendMessage2StorerRequest;
import org.catmq.protocol.service.SendMessage2StorerResponse;
import org.catmq.protocol.definition.NumberedMessage;
import org.catmq.storage.segment.Segment;
import org.catmq.storer.Storer;

import java.util.ArrayList;
import java.util.List;

import static org.catmq.entity.StorerConfig.STORER_CONFIG;

@Slf4j
public class WriteProcessor implements Processor<SendMessage2StorerRequest, SendMessage2StorerResponse> {
    Storer storer = Storer.STORER;



    @Override
    public SendMessage2StorerResponse process(RequestContext ctx, SendMessage2StorerRequest request) {
        if (request.getMessage(0).getEntryId() == 1) {
            storer.getSegmentStorage().getSegments().put(request.getMessage(0).getSegmentId(),
                    new Segment(request.getMessage(0).getSegmentId()));
        }
        if (request.getMessageList().size() == 1) {
            processSingleMessage(ctx, request);
        }
        else {
            processMultiMessage(ctx, request);
        }
        SendMessage2StorerResponse response = SendMessage2StorerResponse.newBuilder().setAck(true).setRes("Success").build();
        return response;
    }

    private void processSingleMessage(RequestContext ctx, SendMessage2StorerRequest request) {
        NumberedMessage nm = request.getMessage(0);
        MessageEntry messageEntry = new MessageEntry(nm.getEntryId(), nm.getSegmentId(), nm.getBody().toByteArray());
        storer.getSegmentStorage().appendEntry2WriteCache(messageEntry);
        storer.getFlushMessageEntryService().putMessageEntry2Queue(messageEntry);
        if (STORER_CONFIG.getFlushMode() == FlushMode.SYNC) {
            try {
                messageEntry.getWaiter().await();
            } catch (InterruptedException e) {
                log.warn("Interrupted", e);
            }
        }
    }

    private void processMultiMessage(RequestContext ctx, SendMessage2StorerRequest request) {
        List<MessageEntry> messageEntries = conv2MessageEntryList(request.getMessageList());
        storer.getSegmentStorage().batchAppendEntry2WriteCache(messageEntries);
        storer.getFlushMessageEntryService().batchPutMessageEntry2Queue(messageEntries);

        if (STORER_CONFIG.getFlushMode() == FlushMode.SYNC) {
            try {
                for (MessageEntry me: messageEntries) {
                    me.getWaiter().await();
                }
            } catch (InterruptedException e) {
                log.warn("Interrupted", e);
            }
        }
    }

    private List<MessageEntry> conv2MessageEntryList(List<NumberedMessage> numberedMessages) {
        List<MessageEntry> messageEntries = new ArrayList<>(numberedMessages.size());
        for (NumberedMessage nm: numberedMessages) {
            messageEntries.add(new MessageEntry(nm.getEntryId(), nm.getSegmentId(), nm.getBody().toByteArray()));
        }
        return messageEntries;
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
