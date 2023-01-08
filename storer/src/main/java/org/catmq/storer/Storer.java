package org.catmq.storer;

import org.catmq.storage.ChunkStorage;
import org.catmq.storage.FlushMessageEntryService;
import org.catmq.storage.MessageLogStorage;

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
