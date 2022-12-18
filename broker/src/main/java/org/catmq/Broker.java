package org.catmq;

import org.catmq.broker.BrokerConfig;
import org.catmq.broker.BrokerInfo;
import org.catmq.broker.zk.BaseZooKeeper;
import org.catmq.broker.zk.HAHelper;
import org.catmq.util.ThreadFactoryWithIndex;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author BYL
 */
public class Broker {

    public BrokerInfo brokerInfo;
    protected BaseZooKeeper bzk;

    private ScheduledExecutorService timeExecutor;
    private ExecutorService publicExecutor;


    public void start() throws IOException {
        this.timeExecutor.scheduleWithFixedDelay(() -> {
            try {
                Set<String> persistentBroker = new HashSet<>(this.bzk.getAllBrokerPaths(true));
                Set<String> ephemeralBroker = new HashSet<>(this.bzk.getAllBrokerPaths(false));
                persistentBroker.removeAll(ephemeralBroker);
                persistentBroker
                        .stream()
                        .filter(name -> !"tmp".equals(name))
                        .forEach(name -> {
                            this.publicExecutor.submit(() -> {
                                HAHelper.handleDeadBroker(this.bzk, name);
                            });
                        });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, 10, TimeUnit.SECONDS);

        this.bzk.register2Zk();
        System.in.read();
    }

    public Broker(BrokerConfig config) throws IOException {
        this.brokerInfo = new BrokerInfo(config.BROKER_ID, config.BROKER_NAME, config.BROKER_IP, config.BROKER_PORT);
        this.bzk = new BaseZooKeeper("127.0.0.1:2181", this);
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
