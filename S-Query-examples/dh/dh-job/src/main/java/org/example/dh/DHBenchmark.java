package org.example.dh;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;
import org.example.dh.events.Event;
import org.example.dh.events.OrderInfo;
import org.example.dh.events.OrderStatus;
import org.example.dh.events.RiderLocation;
import org.example.dh.state.OrderInfoState;
import org.example.dh.state.RiderLocationState;
import org.example.dh.state.OrderStatusState;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.example.dh.EventSourceP.orderInfoSource;
import static org.example.dh.EventSourceP.orderStatusSource;
import static org.example.dh.EventSourceP.riderLocationSource;


public class DHBenchmark extends Benchmark {
    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(Pipeline pipeline, Properties props, HazelcastInstance hz) throws ValidationException {
        int numDistinctOrderIds = parseIntProp(props, PROP_NUM_DISTINCT_ORDER_IDS);
        int eventsPerSecond = parseIntProp(props, PROPS_EVENTS_PER_SECOND);

        // Generate order state events at rate eventPerSecond / 2
        // Generate rider location events at rate eventPerSecond / 2
        // Generate order info state events at rate eventPerSecond / 2 / 10
        StreamStage<OrderInfo> orderInfoSource = pipeline
                .readFrom(orderInfoSource(eventsPerSecond / 2 / 10, INITIAL_SOURCE_DELAY_MILLIS, numDistinctOrderIds))
                .withNativeTimestamps(0);
        StreamStage<OrderStatus> orderStatusSource = pipeline
                .readFrom(orderStatusSource(eventsPerSecond / 2, INITIAL_SOURCE_DELAY_MILLIS, numDistinctOrderIds))
                .withNativeTimestamps(0);
        StreamStage<RiderLocation> riderLocationSource = pipeline
                .readFrom(riderLocationSource(eventsPerSecond / 2, INITIAL_SOURCE_DELAY_MILLIS, numDistinctOrderIds))
                .withNativeTimestamps(0);

        // Payment processor, outputs: (payment status, timestamp)
        StreamStage<Event> orderInfoProcessor = orderInfoSource .groupingKey(orderInfo1 -> orderInfo1.orderId)
                .mapStateful(
//                        SECONDS.toMillis(5), // 5 second TTL
                        OrderInfoState::new,
                        (state, key, orderInfo) -> {
                            state.setLongitudeVendor(orderInfo.longitudeVendor);
                            state.setLatitudeVendor(orderInfo.latitudeVendor);
                            state.setLongitudeCustomer(orderInfo.longitudeCustomer);
                            state.setLatitudeCustomer(orderInfo.latitudeCustomer);
                            state.setLongitudeDeliveryZone(orderInfo.longitudeDeliveryZone);
                            state.setLatitudeDeliveryZone(orderInfo.latitudeDeliveryZone);
                            state.setDeliveryZone(orderInfo.deliveryZone);
                            state.setVendorCategory(orderInfo.vendorCategory);
                            state.setPromisedDeliveryTimestamp(LocalDateTime.ofInstant(Instant.ofEpochMilli(orderInfo.promisedDeliveryTimestamp), ZoneId.systemDefault()));
                            state.setCommittedPickupAtTimestamp(LocalDateTime.ofInstant(Instant.ofEpochMilli(orderInfo.committedPickupAtTimestamp), ZoneId.systemDefault()));
                            return (Event)orderInfo;
                        }
//                        ,(state, key, watermark) -> null // Do nothing on evict
                )
                .setName("orderinfo");

        // Rider location processor, outputs: (order size, total price, and timestamp)
        StreamStage<Event> riderLocationProcessor = riderLocationSource.groupingKey(RiderLocation::getOrderId)
                .mapStateful(
//                        SECONDS.toMillis(5), // 5 second TTL
                        RiderLocationState::new,
                        (state, key, riderLocation) -> {
                           state.setUpdateTimestamp(LocalDateTime.ofInstant(Instant.ofEpochMilli(riderLocation.getUpdateTimestamp()), ZoneId.systemDefault()));
                           state.setLongitude(riderLocation.getLongitude());
                           state.setLatitude(riderLocation.getLatitude());
                           return (Event)riderLocation;
                        }
//                        ,(state, key, currentWatermark) -> null // Do nothing on evict
                )
                .setName("riderlocation");

        // Payment processor, outputs: (payment status, timestamp)
        StreamStage<Event> orderStatusProcessor = orderStatusSource.groupingKey(OrderStatus::getOrderId)
                .mapStateful(
//                        MILLISECONDS.toMillis(1500),
                        OrderStatusState::new,
                        (state, key, orderStatus) -> {
                            state.updateOrderState(orderStatus.getOrderState(), orderStatus.getUpdateTimestamp());
                            return (Event)orderStatus;
                        }
//                        ,(state, key, watermark) -> null // Do nothing on evict
                )
                .setName("orderstate");

        // Measure latencies for order info and rider and order status
        var orderInfoLatencies = orderInfoProcessor.apply(determineLatency(Event::timestamp));
        var riderLocationLatencies = riderLocationProcessor.apply(determineLatency(Event::timestamp));
        var orderStatusLatencies = orderStatusProcessor.apply(determineLatency(Event::timestamp));

        // Combine latencies
        return orderInfoLatencies.merge(riderLocationLatencies).merge(orderStatusLatencies);
    }
}
