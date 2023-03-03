package org.catmq.pipline.processor;

import lombok.extern.slf4j.Slf4j;
import org.catmq.common.MessageEntry;
import org.catmq.grpc.RequestContext;
import org.catmq.pipline.Processor;
import org.catmq.protocol.service.GetMessageFromStorerRequest;
import org.catmq.protocol.service.GetMessageFromStorerResponse;
import org.catmq.storage.segment.SegmentStorage;
import org.catmq.storer.Storer;

import java.util.Optional;

@Slf4j
public class ReadProcessor implements Processor<GetMessageFromStorerRequest, GetMessageFromStorerResponse> {
    Storer storer = Storer.STORER;
    SegmentStorage segmentStorage = storer.getSegmentStorage();


    @Override
    public GetMessageFromStorerResponse process(RequestContext ctx, GetMessageFromStorerRequest request) {
        long segmentId = request.getSegmentId();
        long entryId = request.getEntryId();
        var builder = GetMessageFromStorerResponse.newBuilder().setAck(true);
        //TODO batch read
        Optional<MessageEntry> entry;
        // 1. query from write cache
        entry = segmentStorage.getEntryFromWriteCacheById(segmentId, entryId);
        if (entry.isPresent()) {
            log.debug("read {}@{} from write cache", segmentId, entryId);
            return builder.addMessage(entry.get().conv2NumberedMessage()).build();
        }
        // 2. query from read cache
        entry = segmentStorage.getEntryFromReadCacheById(segmentId, entryId);
        if (entry.isPresent()) {
            log.debug("read {}@{} from read cache", segmentId, entryId);
            return builder.addMessage(entry.get().conv2NumberedMessage()).build();
        }
        // 3. query from db and load into read cache
        entry = segmentStorage.getEntryFromFileById(segmentId, entryId);
        if (entry.isPresent()) {
            log.debug("read {}@{} from file", segmentId, entryId);
            return builder.addMessage(entry.get().conv2NumberedMessage()).build();
        }
        log.error("{}@{} message not found", segmentId, entryId);
        return builder
                .setAck(false)
                .setRes(String.format("[segmentId %d][entryId %d] not found", segmentId, entryId))
                .build();
    }


    public enum ReadProcessorEnum {
        INSTANCE;
        private final ReadProcessor instance = new ReadProcessor();

        public ReadProcessor getInstance() {
            return instance;
        }
    }
}
