package org.catmq.thread;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
/**
 * For mapping thread ids to thread pools and threads within those pools
 * or just for lone named threads. Thread scoped metrics add labels to
 * metrics by retrieving the ThreadPoolThread object from this registry.
 * For flexibility, this registry is not based on TLS.
 */
public class ThreadRegistry {
    private static ConcurrentMap<Long, ThreadPoolThread> threadPoolMap = new ConcurrentHashMap<>();

    /*
        Threads can register themselves as their first act before carrying out
        any work.
     */
    public static void register(String threadPool, int threadPoolThread) {
        register(threadPool, threadPoolThread, Thread.currentThread().getId());
    }

    /*
        Thread factories can register a thread by its id.
     */
    public static void register(String threadPool, int threadPoolThread, long threadId) {
        ThreadPoolThread tpt = new ThreadPoolThread(threadPool, threadPoolThread, threadId);
        threadPoolMap.put(threadId, tpt);
    }

    /*
        Clears all stored thread state.
     */
    public static void clear() {
        threadPoolMap.clear();
    }

    /*
        Retrieves the registered ThreadPoolThread (if registered) for the calling thread.
     */
    public static ThreadPoolThread get() {
        return threadPoolMap.get(Thread.currentThread().getId());
    }

    /**
     * Stores the thread pool and ordinal.
     */
    public static final class ThreadPoolThread {
        final String threadPool;
        final int ordinal;
        final long threadId;

        public ThreadPoolThread(String threadPool, int ordinal, long threadId) {
            this.threadPool = threadPool;
            this.ordinal = ordinal;
            this.threadId = threadId;
        }

        public String getThreadPool() {
            return threadPool;
        }

        public int getOrdinal() {
            return ordinal;
        }
    }
}
