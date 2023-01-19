package org.catmq.storer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.catmq.storage.segment.SegmentStorage;
import org.catmq.storage.messageLog.FlushMessageEntryService;
import org.catmq.storage.messageLog.MessageLogStorage;

@Slf4j
@Getter
public class Storer {

    public static final Storer STORER;

    static {
        STORER = new Storer();
    }

    private Storer() {
    }

    public void init(){
        storerInfo = new StorerInfo();
        messageLogStorage = new MessageLogStorage();
        segmentStorage = new SegmentStorage();
        flushMessageEntryService = new FlushMessageEntryService();
        flushMessageEntryService.start();
    }
    private StorerInfo storerInfo;
    public MessageLogStorage messageLogStorage;
    public SegmentStorage segmentStorage;
    public FlushMessageEntryService flushMessageEntryService;



}
