package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.logging.Logger;

import static java.util.concurrent.TimeUnit.SECONDS;

public class MainTwoCounterJob {
    private static JetInstance jet;
    public static void main(String[] args) {
        JobConfig config = new JobConfig();
        config.setName("two-counter");
        config.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        config.setSnapshotIntervalMillis(SECONDS.toMillis(10)); // Snapshot every 2s
        Logger.getGlobal().info("Initial snapshot name: " + config.getInitialSnapshotName());
        System.out.println("Initial snapshot name: " + config.getInitialSnapshotName());
        Pipeline p = Pipeline.create();
        StreamStage<ItemEvent> src = p
                .readFrom(ItemEvent.itemStream(2, 10, 100)) // Stream of increasing numbers: 0,1,2,3,...
                .withNativeTimestamps(SECONDS.toMillis(5)); // Use native timestamps)
        src
                .groupingKey(ItemEvent::getId1)
                .mapStateful(
                        // The time the state can live before being evicted
                        SECONDS.toMillis(10),
                        // Method creating a new state object (for each unique groupingKey)
                        CounterState::new,
                        // Method that maps a given event and corresponding key to the new output given the state
                        (state, key, itemEvent) -> {
                            state.increaseCounter(itemEvent.getAmount());
                            return String.format("ID: %d, %s", key, state.getCounter());
                        },
                        // Method that executes when states belonging to a key are evicted by watermarks
                        (state, key, currentWatermark) -> "Evicted key: " + key
                ).setName("id1-map")
                .writeTo(Sinks.logger());

        src
                .groupingKey(ItemEvent::getId2)
                .mapStateful(
                        // The time the state can live before being evicted
                        SECONDS.toMillis(10),
                        // Method creating a new state object (for each unique groupingKey)
                        CounterState::new,
                        // Method that maps a given event and corresponding key to the new output given the state
                        (state, key, itemEvent) -> {
                            state.increaseCounter(itemEvent.getAmount());
                            return String.format("ID: %d, %s", key, state.getCounter());
                        },
                        // Method that executes when states belonging to a key are evicted by watermarks
                        (state, key, currentWatermark) -> "Evicted key: " + key
                ).setName("id2-map")
                .writeTo(Sinks.logger());

        jet = Jet.bootstrappedInstance();
        jet.newJob(p, config).join();
    }
}
