package org.catmq.broker.zk;

import org.apache.curator.test.TestingServer;
import org.catmq.broker.BrokerConfig;
import org.catmq.broker.BrokerInfo;
import org.catmq.broker.BrokerServer;
import org.catmq.zk.BrokerZooKeeper;
import org.junit.*;
import org.mockito.Mockito;

import java.io.IOException;

import static org.catmq.broker.BrokerConfig.BROKER_CONFIG;

@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class BrokerZooKeeperTest {

    static TestingServer server;
    static BrokerConfig config;

    @BeforeClass
    public static void beforeClass() throws Exception {
        server = new TestingServer(2222);
        config = BROKER_CONFIG;
//        config.setBrokerId("1");
        config.setBrokerName("broker1");
        config.setBrokerIp("185.25.12.2");
        config.setBrokerPort(5464);
    }

    @Test
    public void testRegister2Zk() throws Exception {
        BrokerZooKeeper bzk = createFakeZk(new BrokerInfo(config));
        bzk.register2Zk();
        Assert.assertNotNull(bzk.client.checkExists().forPath("/broker/" + bzk.getBroker().brokerInfo.getBrokerName()));
        Assert.assertNotNull(bzk.client.checkExists().forPath("/broker/tmp/" + bzk.getBroker().brokerInfo.getBrokerName()));
        bzk.close();
    }


    private BrokerZooKeeper createFakeZk(BrokerInfo info) throws IOException {
        BrokerServer broker = Mockito.mock(BrokerServer.class);
        broker.brokerInfo = info;
        return new BrokerZooKeeper(server.getConnectString(), broker);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        server.close();
    }
}
