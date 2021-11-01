package org.example;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.jet.impl.processor.IMapStateHelper;
import com.hazelcast.jet.impl.processor.SnapshotIMapKey;
import com.hazelcast.map.IMap;
import org.example.events.ChangeOrder;
import org.example.events.ChangeStock;
import org.example.events.Payment;
import org.example.events.PaymentOrder;
import org.example.state.OrderState;
import org.example.state.OrderStateSerializer;
import org.example.state.PaymentState;
import org.example.state.PaymentStateSerializer;
import org.example.state.StockState;
import org.example.state.StockStateSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DirectQueryJob {
    /**
     * Main method.
     * Run from hazelcast-dir/config for all keys on 2 threads:
     * java -cp "../lib/*" org.example.DirectQueryJob payment OrderPaymentBenchmark 0 5 true
     * java -cp "../lib/*" org.example.DirectQueryJob payment OrderPaymentBenchmark 0 180 true 2>&1 | tee -i direct-query-payment-100k-180.txt
     * java -cp "../lib/*" org.example.DirectQueryJob order OrderPaymentBenchmark 0 180 true 2>&1 | tee -i direct-query-order-100k-180.txt
     * java -cp "../lib/*" org.example.DirectQueryJob stock OrderPaymentBenchmark 0 180 true 2>&1 | tee -i direct-query-stock-100k-180.txt
     * @param args Array of arguments:
     *             1. name of vertex to query snapshot state of
     *             2. Job name
     *             3. Amount of keys to query (nothing or 0 means all)
     *             4. Amount of threads to query with (1 or higher)
     *             5. getAll or predicate (true=getAll, false=predicate)
     */
    public static void main(String[] args) {
        if (args.length <= 1 || args.length > 5) {
            throw new IllegalArgumentException("Amount of arguments must be 2, 3, 4 or 5");
        }
        String vertex = args[0];
        String job = args[1];
        int amountOfKeys = 0;
        if (args.length >= 3) {
            amountOfKeys = Integer.parseInt(args[2]);
            if (amountOfKeys < 0) {
                throw new IllegalArgumentException("Amount of keys must be 0 or higher!");
            }
        }
        int finalAmountOfKeys = amountOfKeys;
        int concurrentThreads = 1;
        if (args.length >= 4) {
            concurrentThreads = Integer.parseInt(args[3]);
            if (concurrentThreads <= 0) {
                throw new IllegalArgumentException("Amount of threads must be 1 or higher!");
            }
        }
        final int finalConcurrentThreads = concurrentThreads;
        boolean getAll = false;
        if (args.length >= 5) {
            getAll = Boolean.parseBoolean(args[4]);
        }
        boolean finalGetAll = getAll;

        String getAllString = "getAll";
        if (!finalGetAll) {
            getAllString = "predicate";
        }
        System.out.printf(
                "Running query on stateful transform '%s' on job '%s' limited to %d keys on %d threads with %s%n",
                vertex, job, amountOfKeys, finalConcurrentThreads, getAllString);

        ClientConfig config = ClientConfig.load();
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig().setTypeClass(Payment.class).setImplementation(new Payment.PaymentSerializer()));
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig().setTypeClass(PaymentOrder.class).setImplementation(new PaymentOrder.PaymentOrderSerializer()));
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig().setTypeClass(ChangeOrder.class).setImplementation(new ChangeOrder.ChangeOrderSerializer()));
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig().setTypeClass(ChangeStock.class).setImplementation(new ChangeStock.ChangeStockSerializer()));
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig().setTypeClass(OrderState.class).setImplementation(new OrderStateSerializer()));
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig().setTypeClass(PaymentState.class).setImplementation(new PaymentStateSerializer()));
        config.getSerializationConfig().addSerializerConfig(new SerializerConfig().setTypeClass(StockState.class).setImplementation(new StockStateSerializer()));

        HazelcastInstance hz = HazelcastClient.newHazelcastClient(config);

        String snapshotMapName = IMapStateHelper.getPhaseSnapshotMapName(vertex);
        String snapshotIdName = IMapStateHelper.getSnapshotIdName(job);

        IMap<SnapshotIMapKey<Long>, Object> imap = hz.getMap(snapshotMapName);
        if (imap.size() == 0) {
            System.err.println("Map has 0 items");
            throw new IllegalArgumentException("Map contains 0 items");
        }

        IAtomicLong snapshotId = hz.getCPSubsystem().getAtomicLong(snapshotIdName);

        List<Long> queryLatencies = Collections.synchronizedList(new ArrayList<>());
        List<Long> snapshotIdLatencies = Collections.synchronizedList(new ArrayList<>());
        final AtomicBoolean stop = new AtomicBoolean(false);

        ExecutorService threadPool = Executors.newFixedThreadPool(finalConcurrentThreads);
        long beforeAll = System.nanoTime();
        for (int i = 0; i < finalConcurrentThreads; i++) {
            threadPool.submit(() -> {
                Collection<Object> result;
                while(!stop.get()) {
                    long beforeSnapshotId = System.nanoTime();
                    long latestSnapshotId = snapshotId.get();
                    long beforeGetAll = System.nanoTime();
                    if (finalAmountOfKeys == 0) {
                        result = imap.values(new SnapshotPredicate(latestSnapshotId));
                    } else {
                        if (finalGetAll) {
                            if (finalAmountOfKeys == 1) {
                                result = Collections.singleton(imap.get(new SnapshotIMapKey<>(0L, latestSnapshotId)));
                            } else {
                                HashSet<SnapshotIMapKey<Long>> keys = new HashSet<>(finalAmountOfKeys);
                                for (long key = 0; key < finalAmountOfKeys; key++) {
                                    keys.add(new SnapshotIMapKey<>(key, latestSnapshotId));
                                }
                                result = imap.getAll(keys).values();
                            }
                        } else {
                            result = imap.values(new SnapshotRangePredicate(latestSnapshotId, finalAmountOfKeys));
                        }
                    }
                    long afterGetAll = System.nanoTime();
                    int size = result.size();
                    if (size != finalAmountOfKeys && finalAmountOfKeys != 0) {
                        String msg = String.format(
                                "Got unexpected amount of results: %d, expected: %d", size, finalAmountOfKeys);
                        System.err.println(msg);
                    }
                    long nullAmount = result.stream().filter(Objects::isNull).count();
                    if (nullAmount > 0) {
                        System.err.println("Got null: " + nullAmount);
                    }
                    long snapshotDelta = beforeGetAll - beforeSnapshotId;
                    long queryDelta = afterGetAll - beforeGetAll;
                    snapshotIdLatencies.add(snapshotDelta);
                    queryLatencies.add(queryDelta);
                }
            });
        }
        // No more jobs to submit so shutdown the pool
        threadPool.shutdown();
        // On cancellation, stop the threads and print the latencies
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stop.set(true);
            try {
                boolean success = threadPool.awaitTermination(10, TimeUnit.SECONDS);
                printLatencies(snapshotIdLatencies, queryLatencies, beforeAll, finalConcurrentThreads);
                if (!success) {
                    System.err.println("Timeout while awaiting thread termination");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.err.println("Interrupted while awaiting thread termination");
            }
        }));
        // Sleep loop to prevent job from terminating
        while (!stop.get()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static <A, B> List<Map.Entry<A, B>> zip(List<A> as, List<B> bs) {
        if (as.size() != bs.size()) {
            throw new IllegalArgumentException("List are not equal size");
        }
        return IntStream.range(0, as.size())
                .mapToObj(i -> Map.entry(as.get(i), bs.get(i)))
                .collect(Collectors.toList());
    }

    private static void printLatencies(List<Long> snapshotIdLatencies, List<Long> queryLatencies, long beforeAll, int concurrentThreads) {
        long timeDelta = System.nanoTime() - beforeAll;
        System.out.println();
        System.out.println("SSID latencies");
        System.out.println(snapshotIdLatencies);
        System.out.println("Query latencies");
        System.out.println(queryLatencies);
        System.out.println("Total time");
        System.out.println(timeDelta);
        long rawLatencySum = zip(snapshotIdLatencies, queryLatencies).stream().mapToLong(sAndQ -> (sAndQ.getKey() + sAndQ.getValue())).sum()/concurrentThreads;
        System.out.println("Total query time");
        System.out.println(rawLatencySum);
        System.out.println("Total latencies");
        int size = queryLatencies.size();
        System.out.println(size);
        System.out.println("Q/s total time");
        System.out.println((double)size/ (timeDelta/1000000000.0));
        System.out.println("Q/s raw time");
        System.out.println((double)size/ (rawLatencySum/1000000000.0));
    }
}
