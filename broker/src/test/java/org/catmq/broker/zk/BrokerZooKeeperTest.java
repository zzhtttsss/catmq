package org.catmq.broker.zk;

import org.apache.curator.test.TestingServer;
import org.catmq.Broker;
import org.catmq.broker.BrokerInfo;
import org.catmq.command.BooleanError;
import org.catmq.zk.BrokerZooKeeper;
import org.catmq.zk.balance.ILoadBalance;
import org.junit.*;
import org.mockito.Mockito;

import java.io.IOException;

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
        bzk.close();
    }


    private BrokerZooKeeper createFakeZk(BrokerInfo info) throws IOException {
        Broker broker = Mockito.mock(Broker.class);
        ILoadBalance loadBalance = Mockito.mock(ILoadBalance.class);
        Mockito.when(loadBalance.registerConnection(info)).thenReturn(BooleanError.ok());
        broker.brokerInfo = info;
        return new BrokerZooKeeper(server.getConnectString(), broker, loadBalance);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        server.close();
    }
}
