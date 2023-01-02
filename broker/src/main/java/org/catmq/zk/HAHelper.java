package org.catmq.zk;

import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.catmq.broker.BrokerInfo;
import org.catmq.constant.FileConstant;
import org.catmq.constant.ZkConstant;
import org.catmq.entity.ISerialization;

/**
 * This class should be in HA module
 *
 * @author BYL
 */
@Slf4j
public class HAHelper {

    public static void handleDeadBroker(BrokerZooKeeper bzk, String name) {
        log.warn("{} gets dead broker {}. Start to transfer....",
                bzk.getBroker().brokerInfo.getBrokerName(), name);
        String path = StrUtil.concat(true, ZkConstant.BROKER_ROOT_PATH, FileConstant.LEFT_SLASH, name);
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
