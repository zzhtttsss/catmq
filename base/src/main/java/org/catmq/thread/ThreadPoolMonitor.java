package org.catmq.thread;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadPoolMonitor {
    private static Logger jstackLogger = LoggerFactory.getLogger(ThreadPoolMonitor.class);
    private static Logger waterMarkLogger = LoggerFactory.getLogger(ThreadPoolMonitor.class);

    private static final List<ThreadPoolWrapper> MONITOR_EXECUTOR = new CopyOnWriteArrayList<>();
    private static final ScheduledExecutorService MONITOR_SCHEDULED = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat("ThreadPoolMonitor-%d").build()
    );

    private static volatile long threadPoolStatusPeriodTime = TimeUnit.SECONDS.toMillis(3);
    private static volatile boolean enablePrintJstack = true;
    private static volatile long jstackPeriodTime = 60000;
    private static volatile long jstackTime = System.currentTimeMillis();

    public static void config(Logger jstackLoggerConfig, Logger waterMarkLoggerConfig,
                              boolean enablePrintJstack, long jstackPeriodTimeConfig, long threadPoolStatusPeriodTimeConfig) {
        jstackLogger = jstackLoggerConfig;
        waterMarkLogger = waterMarkLoggerConfig;
        threadPoolStatusPeriodTime = threadPoolStatusPeriodTimeConfig;
        ThreadPoolMonitor.enablePrintJstack = enablePrintJstack;
        jstackPeriodTime = jstackPeriodTimeConfig;
    }

    public static ThreadPoolExecutor createAndMonitor(int corePoolSize,
                                                      int maximumPoolSize,
                                                      long keepAliveTime,
                                                      TimeUnit unit,
                                                      String name,
                                                      int queueCapacity) {
        return createAndMonitor(corePoolSize, maximumPoolSize, keepAliveTime, unit, name, queueCapacity, Collections.emptyList());
    }

    public static ThreadPoolExecutor createAndMonitor(int corePoolSize,
                                                      int maximumPoolSize,
                                                      long keepAliveTime,
                                                      TimeUnit unit,
                                                      String name,
                                                      int queueCapacity,
                                                      ThreadPoolStatusMonitor... threadPoolStatusMonitors) {
        return createAndMonitor(corePoolSize, maximumPoolSize, keepAliveTime, unit, name, queueCapacity,
                Lists.newArrayList(threadPoolStatusMonitors));
    }

    public static ThreadPoolExecutor createAndMonitor(int corePoolSize,
                                                      int maximumPoolSize,
                                                      long keepAliveTime,
                                                      TimeUnit unit,
                                                      String name,
                                                      int queueCapacity,
                                                      List<ThreadPoolStatusMonitor> threadPoolStatusMonitors) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                unit,
                new LinkedBlockingQueue<>(queueCapacity),
                new ThreadFactoryBuilder().setNameFormat(name + "-%d").build(),
                new ThreadPoolExecutor.DiscardOldestPolicy());
        List<ThreadPoolStatusMonitor> printers = Lists.newArrayList(new ThreadPoolQueueSizeMonitor(queueCapacity));
        printers.addAll(threadPoolStatusMonitors);

        MONITOR_EXECUTOR.add(ThreadPoolWrapper.builder()
                .name(name)
                .threadPoolExecutor(executor)
                .statusPrinters(printers)
                .build());
        return executor;
    }

    public static void logThreadPoolStatus() {
        for (ThreadPoolWrapper threadPoolWrapper : MONITOR_EXECUTOR) {
            List<ThreadPoolStatusMonitor> monitors = threadPoolWrapper.getStatusPrinters();
            for (ThreadPoolStatusMonitor monitor : monitors) {
                double value = monitor.value(threadPoolWrapper.getThreadPoolExecutor());
                waterMarkLogger.info("\t{}\t{}\t{}", threadPoolWrapper.getName(),
                        monitor.describe(),
                        value);

                if (enablePrintJstack) {
                    if (monitor.needPrintJstack(threadPoolWrapper.getThreadPoolExecutor(), value) &&
                            System.currentTimeMillis() - jstackTime > jstackPeriodTime) {
                        jstackTime = System.currentTimeMillis();
                        jstackLogger.warn("jstack start\n");
                    }
                }
            }
        }
    }

    public static void init() {
        MONITOR_SCHEDULED.scheduleAtFixedRate(ThreadPoolMonitor::logThreadPoolStatus, 20,
                threadPoolStatusPeriodTime, TimeUnit.MILLISECONDS);
    }

    public static void shutdown() {
        MONITOR_SCHEDULED.shutdown();
    }
}