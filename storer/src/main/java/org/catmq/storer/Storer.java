package org.catmq.storer;

import org.catmq.storage.segment.PartitionSegmentStorage;
import org.catmq.storage.messageLog.FlushMessageEntryService;
import org.catmq.storage.messageLog.MessageLogStorage;

public class Storer {

    public final MessageLogStorage messageLogStorage;
    public final PartitionSegmentStorage partitionSegmentStorage;

    public final FlushMessageEntryService flushMessageEntryService;



    private Storer() {
        messageLogStorage = new MessageLogStorage();
        partitionSegmentStorage = new PartitionSegmentStorage();
        flushMessageEntryService = new FlushMessageEntryService();
        flushMessageEntryService.start();
    }

    public static final Storer STORER = new Storer();






}
