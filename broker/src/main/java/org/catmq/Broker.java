package org.catmq;

import org.catmq.broker.BrokerConfig;
import org.catmq.broker.BrokerInfo;
import org.catmq.util.ThreadFactoryWithIndex;
import org.catmq.zk.BrokerZooKeeper;
import org.catmq.zk.balance.LoadBalanceFactory;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * @author BYL
 */
public class Broker {

    public BrokerInfo brokerInfo;
    protected BrokerZooKeeper bzk;

    private ScheduledExecutorService timeExecutor;
    private ExecutorService publicExecutor;


    public void start() throws IOException {

        this.bzk.register2Zk();
        System.out.println("Broker started");
        System.in.read();
    }

    public Broker(BrokerConfig config) throws IOException {
        this.brokerInfo = new BrokerInfo(config.BROKER_ID, config.BROKER_NAME,
                config.BROKER_IP, config.BROKER_PORT, "127.0.0.1:2181");
        this.bzk = new BrokerZooKeeper("127.0.0.1:2181", this, LoadBalanceFactory.LEAST_USED.getStrategy());
        this.timeExecutor = new ScheduledThreadPoolExecutor(4,
                new ThreadFactoryWithIndex("BrokerTimerThread_"));
        this.publicExecutor = new ThreadPoolExecutor(4, 4, 500L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new ThreadFactoryWithIndex("BrokerPublicThread_"));

    }


    public static void main(String[] args) throws Exception {
        BrokerConfig config = new BrokerConfig("/broker.properties");
        config.BROKER_NAME = args[0];
        Broker broker = new Broker(config);
        broker.start();
    }
}
