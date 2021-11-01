package org.example;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;
import org.example.events.ChangeStock;
import org.example.events.ChangeOrder;
import org.example.events.OrderBase;
import org.example.events.Payment;
import org.example.events.PaymentOrder;
import org.example.state.StockState;
import org.example.state.OrderState;
import org.example.state.PaymentState;

import java.util.Map;
import java.util.Properties;

import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static org.example.EventSourceP.orderSource;
import static org.example.EventSourceP.paymentSource;
import static org.example.EventSourceP.stockSource;

public class OrderPaymentBenchmark extends BenchmarkBase {
    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(Pipeline pipeline, Properties props, HazelcastInstance hz) throws ValidationException {
        int numDistinctOrderIds = parseIntProp(props, PROP_NUM_DISTINCT_ORDER_IDS);
        int paymentsPerSecond = parseIntProp(props, PROPS_PAYMENTS_PER_SECOND);
        int numItemIds = parseIntProp(props, PROP_NUM_DISTINCT_ITEM_IDS);
        int maxItemsInOrder = parseIntProp(props, PROP_MAX_ITEMS_ORDER);
        short maxStockIncrease = (short) parseIntProp(props, PROP_MAX_STOCK_INCREASE);
        int orderChangesPerSecond = paymentsPerSecond * 3;
        int stockIncreasePerSecond = paymentsPerSecond * 3;

        // Generate payments at rate eventPerSecond, each payment refers to orderId = seq / 3
        // Generate order at rate eventsPerSecond*3, each order refers to orderId = seq / 9
        // Generate stock increase events at rate eventsPerSecond*3, each stock event refers to stockId = seq / 9
        StreamStage<OrderBase> orders = pipeline
                .readFrom(orderSource(orderChangesPerSecond, INITIAL_SOURCE_DELAY_MILLIS, numDistinctOrderIds, numItemIds))
                .withNativeTimestamps(0);
        StreamStage<Payment> payments = pipeline
                .readFrom(paymentSource(paymentsPerSecond, INITIAL_SOURCE_DELAY_MILLIS, numDistinctOrderIds))
                .withNativeTimestamps(0);
        StreamStage<ChangeStock> stocks = pipeline
                .readFrom(stockSource(stockIncreasePerSecond, INITIAL_SOURCE_DELAY_MILLIS, numItemIds, maxStockIncrease))
                .withNativeTimestamps(0);

        // Payment processor, outputs: (payment status, timestamp)
        StreamStage<PaymentOrder> paymentProcessor = payments.groupingKey(Payment::getOrderId)
                .mapStateful(
//                        SECONDS.toMillis(5), // 5 second TTL
                        PaymentState::new,
                        (state, key, payment) -> {
                            boolean success = state.setPaymentStatus(payment.getPaymentStatus());
//                            if (success) {
//                                System.out.println("set payment status success");
//                            } else {
//                                System.out.println("set payment status failure");
//                            }
                            if (success) {
                                if (state.getPaymentStatus() == Payment.PaymentStatus.PAID) {
                                    return new PaymentOrder(payment.id(), payment.timestamp(), payment.getOrderId(), true);
                                } else if (state.getPaymentStatus() == Payment.PaymentStatus.REFUNDED) {
                                    return new PaymentOrder(payment.id(), payment.timestamp(), payment.getOrderId(), false);
                                }

                            }
                            return null;
                        }
//                        ,(state, key, watermark) -> null // Do nothing on evict
                )
                .setName("payment");

        // Order processor, outputs: (order size, total price, and timestamp)
        StreamStage<ChangeStock> ordersProcessor = orders.merge(paymentProcessor).groupingKey(OrderBase::getOrderId)
                .flatMapStateful(
//                        SECONDS.toMillis(5), // 5 second TTL
                        OrderState::new,
                        (state, key, orderBase) -> {
                            if (orderBase instanceof ChangeOrder) {
                                // Event is a ChangeOrder
                                ChangeOrder order = (ChangeOrder) orderBase;
                                if (order.getOperation()) {
                                    if (state.getItemCount().size() < maxItemsInOrder || state.getItemCount().containsKey(order.getItemId())) {
                                        state.addItem(order.getItemId());
                                    }
//                                System.out.println(String.format("Successfully added item ID %d to order %d", order.getItemId(), order.getOrderId()));
                                } else {
                                    boolean result = state.removeItem(order.getItemId());
//                                if (result) {
//                                    System.out.println(String.format("Removed item ID %d from order %d", order.getItemId(), order.getOrderId()));
//                                } else {
//                                    System.out.println(String.format("Item ID %d not in order %d", order.getItemId(), order.getOrderId()));
//                                }
                                }
                            } else if (orderBase instanceof PaymentOrder) {
                                // Event is a PaymentOrder
                                PaymentOrder order = (PaymentOrder) orderBase;
                                Map<Long, Short> items =  state.getItemCount();
                                ChangeStock[] changes;
                                if (order.isSuccess()) {
                                    changes = items.entrySet().stream().map(e -> new ChangeStock(
                                            orderBase.id(),
                                            orderBase.timestamp(),
                                            e.getKey(),
                                            e.getValue())).toArray(ChangeStock[]::new);
                                } else {
                                    changes = items.entrySet().stream().map(e -> new ChangeStock(
                                            orderBase.id(),
                                            orderBase.timestamp(),
                                            e.getKey(),
                                            (short) -e.getValue())).toArray(ChangeStock[]::new);
                                    state.clear(); // Reset order after refund
                                }
                                return Traversers.traverseArray(changes);
                            }
                            return Traversers.empty();
                        }
//                        ,(state, key, currentWatermark) -> null // Do nothing on evict
                )
                .setName("order");

        // Payment processor, outputs: (payment status, timestamp)
        StreamStage<Tuple3<Long, Long, Long>> stockProcessor = stocks.merge(ordersProcessor).groupingKey(ChangeStock::getItemId)
                .mapStateful(
//                        SECONDS.toMillis(5), // 5 second TTL
                        StockState::new,
                        (state, key, changeStock) -> {
                            state.deltaStock(changeStock.getStockDelta());
                            return tuple3(changeStock.getItemId(), state.getStock(), changeStock.timestamp());
                        }
//                        ,(state, key, watermark) -> null // Do nothing on evict
                )
                .setName("stock");

        // Measure latencies for order and payment events
//        var latencies = ordersProcessor.apply(determineLatency(Tuple3::f2));
//        var paymentLatencies = paymentProcessor.apply(determineLatency(Tuple2::f1));
        var balanceLatencies = stockProcessor.apply(determineLatency(Tuple3::f2));

        // Combine latencies
//        return latencies.merge(paymentLatencies).merge(balanceLatencies);
        return balanceLatencies;
    }
}
