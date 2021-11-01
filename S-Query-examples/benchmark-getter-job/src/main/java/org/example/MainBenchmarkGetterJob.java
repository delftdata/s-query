package org.example;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.impl.processor.IMapStateHelper;

import java.util.ArrayList;
import java.util.List;

public class MainBenchmarkGetterJob {
    public static void main(String[] args) {
        JetInstance jet = Jet.bootstrappedInstance();
        HazelcastInstance hz = jet.getHazelcastInstance();
        if (args.length > 0) {
            for (int i = 0; i < args.length; i++) {
                BenchMarkLists lists = getBenchmarkLists(args[i], hz);
                System.out.printf("%s benchmark list:%n", args[i]);
                printLists(lists);
            }
        } else {
            System.out.println("User tracking benchmark lists:");
            printLists(getBenchmarkLists("user-tracking", hz));
            System.out.println("User orders benchmark lists:");
            printLists(getBenchmarkLists("user-orders", hz));
        }
    }

    public static class BenchMarkLists {
        public final List<Long> imapBench;
        public final List<Long> phase1Bench;
        public final List<Long> phase2Bench;

        public BenchMarkLists(List<Long> imapBench, List<Long> phase1Bench, List<Long> phase2Bench) {
            this.imapBench = imapBench;
            this.phase1Bench = phase1Bench;
            this.phase2Bench = phase2Bench;
        }
    }

    private static void printLists(BenchMarkLists benchMarkLists) {
        System.out.println("IMap list:");
        System.out.println(benchMarkLists.imapBench);
        System.out.println("Phase 1 list:");
        System.out.println(benchMarkLists.phase1Bench);
        System.out.println("Phase 2 list:");
        System.out.println(benchMarkLists.phase2Bench);
    }

    private static BenchMarkLists getBenchmarkLists(String jobName, HazelcastInstance hz) {
        List<Long> imapBenchmarks = hz.getList(IMapStateHelper.getBenchmarkIMapTimesListName(jobName));
        List<Long> phase1Benchmarks = hz.getList(IMapStateHelper.getBenchmarkPhase1TimesListName(jobName));
        List<Long> phase2Benchmarks = hz.getList(IMapStateHelper.getBenchmarkPhase2TimesListName(jobName));
        imapBenchmarks = new ArrayList<>(imapBenchmarks);
        phase1Benchmarks = new ArrayList<>(phase1Benchmarks);
        phase2Benchmarks = new ArrayList<>(phase2Benchmarks);
        return new BenchMarkLists(imapBenchmarks, phase1Benchmarks, phase2Benchmarks);
    }
}
