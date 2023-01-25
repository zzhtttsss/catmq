package org.catmq.client.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class BatchMessageEntry extends MessageEntry implements Iterable<MessageEntry> {

    private final List<MessageEntry> messages;

    private BatchMessageEntry(List<MessageEntry> messages) {
        this.messages = messages;
    }

    public Iterator<MessageEntry> iterator() {
        return messages.iterator();
    }

    public static BatchMessageEntry generateFromList(Collection<? extends MessageEntry> messages) {
        assert messages != null;
        assert messages.size() > 0;
        List<MessageEntry> messageList = new ArrayList<MessageEntry>(messages.size());
        messageList.addAll(messages);
        return new BatchMessageEntry(messageList);
    }

}