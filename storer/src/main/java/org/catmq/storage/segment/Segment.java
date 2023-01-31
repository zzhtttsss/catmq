package org.catmq.storage.segment;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

import static org.catmq.entity.StorerConfig.STORER_CONFIG;

@Slf4j
public class Segment {

    @Getter
    private long segmentId;

    @Getter
    private AtomicLong entryNum;


    public Segment(long segmentId) {
        this.segmentId = segmentId;
        entryNum = new AtomicLong(0L);
    }



    public boolean isFull(){
        return entryNum.get() == STORER_CONFIG.getMaxSegmentEntryNum();
    }
}
