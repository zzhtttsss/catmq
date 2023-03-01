package org.catmq.client.common;


import lombok.Data;
import org.catmq.util.ConfigUtil;

@Data
public class ClientConfig {
    public String clientIP = ConfigUtil.getLocalAddress();
    
}
