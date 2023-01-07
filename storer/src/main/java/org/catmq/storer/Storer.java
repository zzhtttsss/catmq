package org.catmq.storer;

import org.catmq.storage.ChunkStorage;
import org.catmq.storage.MessageLogStorage;

public class Storer {

    public final MessageLogStorage messageLogStorage;
    public final ChunkStorage chunkStorage;



    private Storer() {
        messageLogStorage = new MessageLogStorage();
        chunkStorage = new ChunkStorage();
    }

    public static final Storer STORER = new Storer();






}
