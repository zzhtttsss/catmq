package org.catmq.remoting.netty;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Spy;

public class AbstractNettyRemotingTest {
    @Spy
    private AbstractNettyRemoting remotingAbstract = new NettyClient();

    @Test
    public void testScanResponseTable() {
        int outdatedId = 1;
        int id = 2;
        ResponseFuture future1 = new ResponseFuture(null, id, null, 3000, null);
        ResponseFuture future2 = new ResponseFuture(null, outdatedId, null, -3000, null);
        remotingAbstract.responseTable.putIfAbsent(id, future1);
        remotingAbstract.responseTable.putIfAbsent(outdatedId, future2);
        remotingAbstract.scanResponseTable();
        Assert.assertEquals(1, remotingAbstract.responseTable.size());
        Assert.assertNull(remotingAbstract.responseTable.get(outdatedId));

    }
}
