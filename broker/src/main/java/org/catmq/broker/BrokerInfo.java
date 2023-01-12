package org.catmq.broker;

import com.alibaba.fastjson2.JSON;
import lombok.Data;
import lombok.NonNull;
import org.catmq.entity.ISerialization;

/**
 * @author HP
 */
@Data
public class BrokerInfo implements ISerialization {
    @NonNull
    private String brokerId;
    private String brokerName;
    @NonNull
    private String brokerIp;
    private int brokerPort;
    @NonNull
    private String zkAddress;
    /**
     * The number of connections on this broker.
     */
    private int load;

    @Override
    public byte[] toBytes() {
        return JSON.toJSONBytes(this);
    }

    public BrokerInfo(BrokerConfig config) {
        this.brokerId = config.getBrokerId();
        this.brokerName = config.getBrokerName();
        this.brokerIp = config.getBrokerIp();
        this.brokerPort = config.getBrokerPort();
        this.load = 0;
    }
}
