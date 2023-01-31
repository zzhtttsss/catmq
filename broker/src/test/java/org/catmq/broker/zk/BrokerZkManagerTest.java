package org.catmq.broker.zk;

import org.junit.*;

@FixMethodOrder(org.junit.runners.MethodSorters.NAME_ASCENDING)
public class BrokerZkManagerTest {

//    static TestingServer server;
//    static BrokerConfig config;
//
//    @BeforeClass
//    public static void beforeClass() throws Exception {
//        server = new TestingServer(2222);
//        config = BROKER_CONFIG;
////        config.setBrokerId("1");
//        config.setBrokerName("broker1");
//        config.setBrokerIp("185.25.12.2");
//        config.setBrokerPort(5464);
//    }
//
//    @Test
//    public void testRegister2Zk() throws Exception {
//        BrokerZkManager bzk = createFakeZk(new BrokerInfo(config));
//        bzk.register2Zk();
//        Assert.assertNotNull(bzk.client.checkExists().forPath("/broker/" + bzk.getBroker().brokerInfo.getBrokerName()));
//        Assert.assertNotNull(bzk.client.checkExists().forPath("/broker/tmp/" + bzk.getBroker().brokerInfo.getBrokerName()));
//        bzk.close();
//    }
//
//
//    private BrokerZkManager createFakeZk(BrokerInfo info) throws IOException {
//        BrokerServer broker = Mockito.mock(BrokerServer.class);
//        broker.brokerInfo = info;
//        return new BrokerZkManager(server.getConnectString(), broker);
//    }
//
//    @AfterClass
//    public static void afterClass() throws Exception {
//        server.close();
//    }
}
