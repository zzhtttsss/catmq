package org.catmq.zk;

import org.catmq.broker.BrokerInfo;
import org.catmq.entity.ISerialization;
import org.slf4j.Logger;

/**
 * This class should be in HA module
 *
 * @author BYL
 */
public class HAHelper {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(HAHelper.class);

    public static void handleDeadBroker(BrokerZooKeeper bzk, String name) {
        log.warn("{} gets dead broker {}. Start to transfer....",
                bzk.getBroker().brokerInfo.getBrokerName(), name);
        String path = "/broker/" + name;
        try {
            byte[] bytes = bzk.client.getData().forPath(path);
            BrokerInfo brokerInfo = ISerialization.fromBytes(bytes, BrokerInfo.class);
            log.info("Get BrokerInfo: {}", brokerInfo);
            bzk.client.delete().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
