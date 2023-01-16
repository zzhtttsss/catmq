package org.catmq.storer;

import lombok.extern.slf4j.Slf4j;
import org.catmq.storage.segment.SegmentStorage;
import org.catmq.storage.messageLog.FlushMessageEntryService;
import org.catmq.storage.messageLog.MessageLogStorage;

@Slf4j
public class Storer {

    public static final Storer STORER;

    public MessageLogStorage messageLogStorage;
    public SegmentStorage segmentStorage;

    public FlushMessageEntryService flushMessageEntryService;

    static {
        STORER = new Storer();
    }

    private Storer() {}

    public void init(){
        messageLogStorage = new MessageLogStorage();
        segmentStorage = new SegmentStorage();
        flushMessageEntryService = new FlushMessageEntryService();
        flushMessageEntryService.start();
    }







}
