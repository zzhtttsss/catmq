package org.catmq.entity;

import com.alibaba.fastjson2.JSON;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@NoArgsConstructor
public class BrokerInfo implements JsonSerializable {
    private long brokerId;
    private String brokerName;
    @NonNull
    private String brokerIp;
    private int brokerPort;
    @NonNull
    private String zkAddress;
    private String brokerAddress;


    /**
     * The number of connections on this broker.
     */
    private int load;

    @Override
    public byte[] toBytes() {
        return JSON.toJSONBytes(this);
    }

    public BrokerInfo(BrokerConfig config) {
        this.brokerName = config.getBrokerName();
        this.brokerIp = config.getBrokerIp();
        this.brokerPort = config.getBrokerPort();
        this.zkAddress = config.getZkAddress();
        this.load = 0;
        // TODO concat
        this.brokerAddress = brokerIp + ":" + brokerPort;
    }
}
