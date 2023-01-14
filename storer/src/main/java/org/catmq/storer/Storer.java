package org.catmq.storer;

import lombok.extern.slf4j.Slf4j;
import org.catmq.storage.segment.PartitionSegmentStorage;
import org.catmq.storage.messageLog.FlushMessageEntryService;
import org.catmq.storage.messageLog.MessageLogStorage;

@Slf4j
public class Storer {

    public static final Storer STORER;

    public MessageLogStorage messageLogStorage;
    public PartitionSegmentStorage partitionSegmentStorage;

    public FlushMessageEntryService flushMessageEntryService;

    static {
        STORER = new Storer();
    }

    private Storer() {}

    public void init(){
        messageLogStorage = new MessageLogStorage();
        partitionSegmentStorage = new PartitionSegmentStorage();
        flushMessageEntryService = new FlushMessageEntryService();
        flushMessageEntryService.start();
    }







}
