package org.example;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;

import java.util.logging.Logger;

import static java.util.concurrent.TimeUnit.SECONDS;

public class MainStreamJob {
    private static JetInstance jet;
    private static HazelcastInstance hz;
    public static void main(String[] args) {
        JobConfig config = new JobConfig();
        config.setName("jet-test");
        config.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        config.setSnapshotIntervalMillis(SECONDS.toMillis(10)); // Snapshot every 2s
        Logger.getGlobal().info("Initial snapshot name: " + config.getInitialSnapshotName());
        System.out.println("Initial snapshot name: " + config.getInitialSnapshotName());
        Pipeline p = Pipeline.create();
        p.readFrom(LoginEvent.itemStream(2, 100, LoginEvent::new)) // Stream of increasing numbers: 0,1,2,3,...
                .withNativeTimestamps(SECONDS.toMillis(5)) // Use native timestamps
                .groupingKey(e -> e.getId()) // Group the events by their sequence number
                .mapStateful(
                        // The time the state can live before being evicted
                        SECONDS.toMillis(10),
                        // Method creating a new state object (for each unique groupingKey)
                        () -> new LoginState(0),
                        // Method that maps a given event and corresponding key to the new output given the state
                        (state, key, event) -> {
                            if (event.getEvent() == LoginEvent.LoginEventEnum.LOGIN) {
                                state.setState(1);
                            } else {
                                state.setState(0);
                            }
                            return String.format("ID: %d, %s", key, state.getState());
                        },
                        // Method that executes when states belonging to a key are evicted by watermarks
                        (state, key, currentWatermark) -> "Evicted key: " + key
                )
                .writeTo(Sinks.logger());
        jet = Jet.bootstrappedInstance();
        jet.newJob(p, config).join();
    }
}
