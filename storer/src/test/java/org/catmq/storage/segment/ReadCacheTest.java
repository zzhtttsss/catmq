package org.catmq.storage.segment;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.catmq.common.MessageEntry;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.ThreadLocalRandom;

import static org.catmq.storer.StorerConfig.STORER_CONFIG;

@State(Scope.Benchmark)
@BenchmarkMode({Mode.Throughput, Mode.AverageTime})
public class ReadCacheTest {

    ReadCache cache;
    String defaultValue;

    @Setup
    public void setup() {
        cache = new ReadCache(STORER_CONFIG.getSegmentMaxFileSize());
        defaultValue = "hello world 2023";
    }

    @Benchmark
    public void put() {
        int key = ThreadLocalRandom.current().nextInt();
        cache.putEntry(new MessageEntry(key, key, defaultValue.getBytes()));
    }

    @Benchmark
    public MessageEntry get() {
        int key = ThreadLocalRandom.current().nextInt();
        return cache.getEntry(key, key);
    }


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ReadCacheTest.class.getSimpleName())
                .forks(1)
                .threads(8)
                .build();

        new Runner(opt).run();
    }

    @Test
    public void testCaffeine() {
        ConcurrentLinkedHashMap<Integer, Integer> map = new ConcurrentLinkedHashMap.Builder<Integer, Integer>()
                .maximumWeightedCapacity(1000)
                .build();
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);
        map.put(4, 4);
        map.get(1);
        map.ascendingKeySet().forEach(k -> {
            System.out.println(k + " " + map.get(k));
        });
    }
}
