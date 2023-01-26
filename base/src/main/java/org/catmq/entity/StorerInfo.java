package org.catmq.entity;

import com.alibaba.fastjson2.JSON;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class StorerInfo implements JsonSerializable {

    @NonNull
    private String storerId;
    private String storerName;
    @NonNull
    private String storerIp;
    private int storerPort;
    private String storerAddress;
    private String zkAddress;
    private int messageLogNum;


    public StorerInfo() {
        storerId = "1";
        storerIp = "127.0.0.1";
        storerName = "first";
        storerPort = 4321;
        zkAddress = "127.0.0.1:2181";
        messageLogNum = 0;
        storerAddress = storerIp + storerPort;
    }

    public byte[] toBytes() {
        return JSON.toJSONBytes(this);
    }

    public void addMessageLogNum() {
        messageLogNum++;
    }
}
