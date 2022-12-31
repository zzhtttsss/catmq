package org.catmq.broker.zk;

import org.apache.curator.test.TestingServer;
import org.catmq.Broker;
import org.catmq.broker.BrokerInfo;
import org.catmq.zk.BrokerZooKeeper;
import org.junit.*;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;

@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class BrokerZooKeeperTest {

    static TestingServer server;

    @BeforeClass
    public static void beforeClass() throws Exception {
        server = new TestingServer(2222);
    }

    @Test
    public void testRegister2Zk() throws Exception {
        BrokerZooKeeper bzk = createFakeZk(new BrokerInfo("id", "test", "1111", 1111));
        bzk.register2Zk();
        Assert.assertNotNull(bzk.client.checkExists().forPath("/broker/" + bzk.getBroker().brokerInfo.getBrokerName()));
        Assert.assertNotNull(bzk.client.checkExists().forPath("/broker/tmp/" + bzk.getBroker().brokerInfo.getBrokerName()));
        List<String> strings = bzk.client.getChildren().forPath("/address/broker");
        Assert.assertEquals(1, strings.size());
        Assert.assertEquals(bzk.getBroker().brokerInfo.getBrokerIp() + ":" + bzk.getBroker().brokerInfo.getBrokerPort(),
                strings.get(0));
        String path = "/address/broker/" + strings.get(0);
        Assert.assertEquals("0", new String(bzk.client.getData().forPath(path)));
        bzk.close();
    }


    private BrokerZooKeeper createFakeZk(BrokerInfo info) throws IOException {
        Broker broker = Mockito.mock(Broker.class);
        broker.brokerInfo = info;
        return new BrokerZooKeeper(server.getConnectString(), broker, null);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        server.close();
    }
}
