package org.catmq.zk;

import com.alibaba.fastjson2.JSON;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.catmq.entity.JsonSerializable;

import java.util.HashMap;
import java.util.Map;

@Data
@NoArgsConstructor
public class TopicZkInfo implements JsonSerializable {
    private String simpleName;
    private String type;



    private int partitionNum;
    private HashMap<Integer, String> brokerZkPaths;

    public TopicZkInfo(String simpleName, String type, int partitionNum, HashMap<Integer, String> brokerZkPaths) {
        this.simpleName = simpleName;
        this.type = type;
        this.partitionNum = partitionNum;
        this.brokerZkPaths = brokerZkPaths;
    }

    @Override
    public byte[] toBytes() {
        return JSON.toJSONBytes(this);
    }

}
