package org.catmq;

import org.catmq.executer.Executer;
import org.checkerframework.checker.units.qual.A;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class BrokerStartup {




    public static void main(String[] args) throws IOException, InterruptedException {
        BrokerServer.start();
        BrokerServer.blockUntilShutdown();
    }
}
