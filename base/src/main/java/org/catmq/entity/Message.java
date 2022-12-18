package org.catmq.entity;

import com.alibaba.fastjson2.JSON;
import lombok.Data;
import org.catmq.constant.StringConstant;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author BYL
 */
@Data
public class Message implements ISerialization {

    public static SimpleDateFormat SDF = new SimpleDateFormat(StringConstant.DATE_FORMAT);

    private long commitLogOffset;

    private long consumeQueueOffset;
    private String topic;
    private String msg;
    private String createTime;

    public Message(String topic, String msg) {
        this.topic = topic;
        this.msg = msg;
        this.createTime = SDF.format(new Date());
    }

    @Override
    public byte[] toBytes() {
        return JSON.toJSONBytes(this);
    }
}
