package org.catmq.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.catmq.constant.FileConstant;
import org.catmq.constant.ZkConstant;
import org.catmq.util.Concat2String;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @author BYL
 */
public class ZkUtil {
    public static CuratorFramework createClient(String zkAddress) {
        RetryPolicy policy = new RetryOneTime(1000);
        CuratorFramework cf = CuratorFrameworkFactory.newClient(zkAddress, policy);
        cf.start();
        return cf;
    }

    public static CuratorFramework createClient(String zkAddress, int timeout, TimeUnit unit) throws InterruptedException {
        RetryPolicy policy = new RetryOneTime(1000);
        CuratorFramework cf = CuratorFrameworkFactory.newClient(zkAddress, policy);
        cf.start();
        if (cf.blockUntilConnected(timeout, unit)) {
            return cf;
        }
        throw new RuntimeException("Can not connect to zk server.");
    }

    /**
     * Get the full path of broker address on zk.
     *
     * @param address the address of broker containing IP and Port.
     * @return /address/broker/ip:port
     */
    public static String getFullBrokerAddressPath(InetSocketAddress address) {
        return Concat2String.builder()
                .concat(ZkConstant.BROKER_ADDRESS_PATH)
                .concat(FileConstant.LEFT_SLASH)
                .concat(address.getHostName())
                .concat(FileConstant.COLON)
                .concat(address.getPort())
                .build();
    }

}
