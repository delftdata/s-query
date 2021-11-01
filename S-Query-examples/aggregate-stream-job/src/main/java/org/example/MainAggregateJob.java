package org.example;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;

import static java.util.concurrent.TimeUnit.SECONDS;

public class MainAggregateJob {
    private static JetInstance jet;
    private static HazelcastInstance hz;
    public static void main(String[] args) {
        JobConfig config = new JobConfig();
        config.setName("jet-aggregate");
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(2))
                .withNativeTimestamps(10L)
                .rollingAggregate(AggregateOperations.counting())
                .writeTo(Sinks.logger());
        jet = Jet.bootstrappedInstance();
        jet.newJob(p, config).join();
    }
}
