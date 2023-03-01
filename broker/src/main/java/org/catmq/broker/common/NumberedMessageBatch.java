package org.catmq.broker.common;

import lombok.Data;
import org.catmq.protocol.definition.NumberedMessage;

import java.util.ArrayList;
import java.util.List;

@Data
public class NumberedMessageBatch {
    private final List<NumberedMessage> batch;
    private long batchSize;

    private final long segmentId;

    public void addMessage(NumberedMessage message) {
        batchSize += message.getBody().size();
        batch.add(message);
    }

    public static NumberedMessageBatch of(long segmentId, NumberedMessage... messages) {
        NumberedMessageBatch batch = new NumberedMessageBatch(segmentId);
        for (NumberedMessage message : messages) {
            batch.addMessage(message);
        }
        return batch;
    }

    public static NumberedMessageBatch of(List<NumberedMessage> messageList) {
        NumberedMessageBatch batch = new NumberedMessageBatch(messageList.get(0).getSegmentId());
        batch.batch.addAll(messageList);
        return batch;
    }

    public NumberedMessageBatch(long segmentId) {
        this.segmentId = segmentId;
        this.batch = new ArrayList<>();
        this.batchSize = 0;
    }
}
