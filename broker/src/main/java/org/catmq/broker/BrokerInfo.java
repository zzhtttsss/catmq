package org.catmq.broker;

import com.alibaba.fastjson2.JSON;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.curator.framework.CuratorFramework;
import org.catmq.entity.JsonSerializable;
import org.catmq.zk.ZkIdGenerator;
import org.catmq.zk.ZkUtil;

@Data
@NoArgsConstructor
public class BrokerInfo implements JsonSerializable {
    @NonNull
    private long brokerId;
    private String brokerName;
    @NonNull
    private String brokerIp;
    private int brokerPort;
    @NonNull
    private String zkAddress;

    private CuratorFramework client;

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
        this.client = ZkUtil.createClient(zkAddress);
        this.brokerId = ZkIdGenerator.ZkIdGeneratorEnum.INSTANCE.getInstance().nextId(client);
        this.load = 0;
    }
}
