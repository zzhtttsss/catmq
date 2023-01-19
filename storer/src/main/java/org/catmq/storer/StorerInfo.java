package org.catmq.storer;

import com.alibaba.fastjson2.JSON;
import lombok.Getter;
import lombok.NonNull;

@Getter
public class StorerInfo {

    @NonNull
    private String storerId;
    private String storerName;
    @NonNull
    private String storerIp;
    private int storerPort;
    @NonNull
    private String zkAddress;

    public StorerInfo() {
        storerId = "1";
        storerIp = "127.0.0.1";
        storerName = "first";
        storerPort = 4321;
    }

    public byte[] toBytes() {
        return JSON.toJSONBytes(this);
    }
}
