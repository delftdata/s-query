package org.example;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueryJob {
    private static final AtomicBoolean lock = new AtomicBoolean(false);

    private static boolean getLock() {
        return lock.compareAndSet(false, true);
    }

    private static void releaseLock() {
        lock.set(false);
    }

    private static void printLatencies(List<Long> queryLatencies) {
        while(!getLock()) {
            System.out.println();
            System.out.println("Query latencies");
            System.out.println(queryLatencies);
            releaseLock();
        }
    }

    public static void main(String[] args) {
        JetInstance jet = Jet.bootstrappedInstance();
        HazelcastInstance hz = jet.getHazelcastInstance();
        final AtomicBoolean stop = new AtomicBoolean(false);

        List<Long> queryLatencies = new ArrayList<>();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            printLatencies(queryLatencies);
            stop.set(true);
        }));

        if (args.length == 1) {
            try (SqlResult result = hz.getSql().execute(args[0])) {
                for (SqlRow row : result) {
                    System.out.println(row.getObject(0).toString());
                }
            }
            return;
        }

        if (args.length == 2) {
            int sleepTime = Integer.parseInt(args[1]);
            long start = 0;
            long end = 0;
            while (!stop.get()) {
                start = System.nanoTime();
                try (SqlResult result = hz.getSql().execute(args[0])) {
//                    for (SqlRow row : result) {
//                        System.out.println(row.getObject(0).toString());
//                    }
                } catch (Exception e) {
                    // Do nothing
                } finally {
                    end = System.nanoTime();
                    while(!getLock()) {
                        queryLatencies.add(end - start);
                        start = end;
                        releaseLock();
                    }
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    stop.set(true);
                    e.printStackTrace();
                }
            }
            return;
        }
    }
}
