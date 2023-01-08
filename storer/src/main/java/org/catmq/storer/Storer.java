package org.catmq.storer;

import org.catmq.storage.chunk.ChunkStorage;
import org.catmq.storage.messageLog.FlushMessageEntryService;
import org.catmq.storage.messageLog.MessageLogStorage;

public class Storer {

    public final MessageLogStorage messageLogStorage;
    public final ChunkStorage chunkStorage;

    public final FlushMessageEntryService flushMessageEntryService;



    private Storer() {
        messageLogStorage = new MessageLogStorage();
        chunkStorage = new ChunkStorage();
        flushMessageEntryService = new FlushMessageEntryService();
        flushMessageEntryService.start();
    }

    public static final Storer STORER = new Storer();






}
