package org.catmq.broker;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.catmq.broker.manager.*;
import org.catmq.broker.service.HandleDelayedMessageService;
import org.catmq.broker.service.ScheduleDelayedMessageService;
import org.catmq.collection.TimerTaskList;
import org.catmq.entity.BrokerInfo;
import org.catmq.entity.GrpcConnectManager;
import org.catmq.zk.ZkIdGenerator;
import org.catmq.zk.ZkUtil;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.catmq.entity.BrokerConfig.BROKER_CONFIG;

/**
 * Broker with every service
 */
@Slf4j
@Getter
public class Broker {

    private BrokerInfo brokerInfo;

    private GrpcConnectManager grpcConnectManager;

    private ReadCacheManager readCacheManager;

    private ClientManager clientManager;

    private TopicManager topicManager;

    private StorerManager storerManager;

    private BrokerZkManager brokerZkManager;

    private CuratorFramework client;

    private HandleDelayedMessageService handleDelayedMessageService;

    private ScheduleDelayedMessageService scheduleDelayedMessageService;


    public static final Broker BROKER;

    static {
        BROKER = new Broker();
    }


    private Broker() {


    }

    public void init() {
        this.brokerInfo = new BrokerInfo(BROKER_CONFIG);
        this.client = ZkUtil.createClient(brokerInfo.getZkAddress());
        this.brokerInfo.setBrokerId(ZkIdGenerator.ZkIdGeneratorEnum.INSTANCE.getInstance().nextId(client));
        this.readCacheManager = ReadCacheManager.ReadCacheManagerEnum.INSTANCE.getInstance();
        this.grpcConnectManager = new GrpcConnectManager(100);
        this.clientManager = ClientManager.ClientManagerEnum.INSTANCE.getInstance();
        this.brokerZkManager = BrokerZkManager.BrokerZkManagerEnum.INSTANCE.getInstance();
        brokerZkManager.register2Zk();
        this.topicManager = TopicManager.TopicManagerEnum.INSTANCE.getInstance();
        this.storerManager = StorerManager.StorerManagerEnum.INSTANCE.getInstance();
        BlockingQueue<List<TimerTaskList.TimerTaskEntry>> blockingQueue = new LinkedBlockingQueue<>();
        this.handleDelayedMessageService = new HandleDelayedMessageService(blockingQueue);
        handleDelayedMessageService.start();
        this.scheduleDelayedMessageService = new ScheduleDelayedMessageService(blockingQueue);
        scheduleDelayedMessageService.start();
    }
}




