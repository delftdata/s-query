package org.example.userdemo.order;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.StreamStage;

import static java.util.concurrent.TimeUnit.SECONDS;

public class MainUserOrderJob {
    public static void main(String[] args) {
        JobConfig config = new JobConfig();
        config.setName("user-orders");
        config.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        config.setSnapshotIntervalMillis(SECONDS.toMillis(5)); // Snapshot every 10s
        Pipeline p = Pipeline.create();
        StreamStage<UserOrder> src = p
                .readFrom(UserOrder.itemStream(3, 10, 5)) // Stream of random UserEvents (2 per second)
                .withNativeTimestamps(SECONDS.toMillis(5)); // Use native timestamps)
        src
                .groupingKey(UserOrder::getUserId)
                .mapStateful(
                        // The time the state can live before being evicted
                        0,
//                        SECONDS.toMillis(10),
                        // Method creating a new state object (for each unique groupingKey)
                        OrderState::new,
                        // Method that maps a given event and corresponding key to the new output given the state
                        (state, key, userOrder) -> {
                            state.increaseOrders(userOrder.getCategory(), userOrder.getAmount());
                            return String.format("Total orders for %s: %d", key, state.totalOrders());
                        },
                        // Method that executes when states belonging to a key are evicted by watermarks
                        (state, key, currentWatermark) -> "Evicted key: " + key
                ).setName("order_counter")
                .writeTo(Sinks.logger());

        JetInstance jet = Jet.bootstrappedInstance();
        jet.newJob(p, config).join();
    }
}
