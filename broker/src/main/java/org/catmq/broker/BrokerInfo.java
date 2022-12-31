package org.catmq.broker;

import com.alibaba.fastjson2.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import org.catmq.entity.ISerialization;

/**
 * @author HP
 */
@Data
@AllArgsConstructor
public class BrokerInfo implements ISerialization {
    @NonNull
    private String brokerId;
    private String brokerName;
    @NonNull
    private String brokerIp;
    private int brokerPort;
    @NonNull
    private String zkAddress;

    @Override
    public byte[] toBytes() {
        return JSON.toJSONBytes(this);
    }

    public BrokerInfo(@NonNull String brokerId, String brokerName, @NonNull String brokerIp, int brokerPort) {
        this.brokerId = brokerId;
        this.brokerName = brokerName;
        this.brokerIp = brokerIp;
        this.brokerPort = brokerPort;
    }
}
