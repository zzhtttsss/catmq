package org.catmq.entity;

import lombok.Data;

import java.util.Date;

/**
 * @author BYL
 */
@Data
public class Topic {
    private String topic;
    private String createTime;

    public Topic(String topic) {
        this.topic = topic;
        this.createTime = Message.SDF.format(new Date());
    }
}
