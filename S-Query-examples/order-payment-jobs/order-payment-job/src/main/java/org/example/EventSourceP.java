package org.example;

import com.hazelcast.cluster.Address;
import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.AppendableTraverser;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamSource;
import org.example.events.ChangeStock;
import org.example.events.ChangeOrder;
import org.example.events.OrderBase;
import org.example.events.Payment;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.example.BenchmarkBase.getRandom;

public class EventSourceP extends AbstractProcessor {
    private static final long THROUGHPUT_REPORTING_THRESHOLD = 3_500_000;

    private static final long SOURCE_THROUGHPUT_REPORTING_PERIOD_MILLIS = 10_000;
    private static final long SIMPLE_TIME_SPAN_MILLIS = HOURS.toMillis(3);
    private static final long THROUGHPUT_REPORT_PERIOD_NANOS =
            MILLISECONDS.toNanos(SOURCE_THROUGHPUT_REPORTING_PERIOD_MILLIS);
    private static final long HICCUP_REPORT_THRESHOLD_MILLIS = 50;
    private static final long WM_LAG_THRESHOLD_MILLIS = 100;

    private final long itemsPerSecond;
    private final long startTime;
    private final long nanoTimeMillisToCurrentTimeMillis = determineTimeOffset();
    private final long wmGranularity;
    private final long wmOffset;
    private final BiFunctionEx<? super Long, ? super Long, ?> createEventFn;
    private final AppendableTraverser<Object> traverser = new AppendableTraverser<>(2);
    private String name;
    private int globalProcessorIndex;
    private int totalParallelism;
    private long emitPeriod;
    private long emitSchedule;
    private long lastReport;
    private long counterAtLastReport;
    private long lastCallNanos;
    private long counter;
    private long lastEmittedWm;
    private long nowNanos;

    <T> EventSourceP(
            long startTime,
            long itemsPerSecond,
            EventTimePolicy<? super T> eventTimePolicy,
            BiFunctionEx<? super Long, ? super Long, ? extends T> createEventFn
    ) {
        this.startTime = MILLISECONDS.toNanos(startTime + nanoTimeMillisToCurrentTimeMillis);
        this.itemsPerSecond = itemsPerSecond;
        this.createEventFn = createEventFn;
        wmGranularity = eventTimePolicy.watermarkThrottlingFrameSize();
        wmOffset = eventTimePolicy.watermarkThrottlingFrameOffset();
    }

    public static StreamSource<Payment> paymentSource(long eventsPerSecond, long initialDelayMs, long numDistinctOrderIds) {
        return eventSource("payments", eventsPerSecond, initialDelayMs,
                (seq, timestamp) -> {
                    short probability = (short) getRandom(seq, 100);
                    short paymentStatus;
                    if (probability <= 87) {
                        paymentStatus = Payment.PaymentStatus.PRE_CHECKOUT;
                    } else if (probability <= 91) {
                        paymentStatus = Payment.PaymentStatus.CHECKOUT;
                    } else if (probability <= 95) {
                        paymentStatus = Payment.PaymentStatus.PAID;
                    } else {
                        paymentStatus = Payment.PaymentStatus.REFUNDED;
                    }
                    return new Payment(seq, timestamp, (seq / 3) % numDistinctOrderIds, paymentStatus);
                });
    }

    public static StreamSource<OrderBase> orderSource(long eventsPerSecond, long initialDelayMs, long numDistinctOrderIds, long numItemIds) {
        return eventSource("orders", eventsPerSecond, initialDelayMs,
                (seq, timestamp) -> {
                    long itemId = getRandom(seq, numItemIds);
                    boolean operation = getRandom(seq, 2) == 0;
                    return new ChangeOrder(seq, timestamp, (seq / 9) % numDistinctOrderIds, itemId, operation);
                });
    }

    public static StreamSource<ChangeStock> stockSource(long eventsPerSecond, long initialDelayMs, long numDistinctItemIds, short maxStockIncrease) {
        return eventSource("stocks", eventsPerSecond, initialDelayMs,
                (seq, timestamp) -> {
                    short refill = (short) getRandom(seq, maxStockIncrease);
                    return new ChangeStock(seq, timestamp, (seq / 9) % numDistinctItemIds, refill);
                });
    }

    @SuppressWarnings("SameParameterValue")
    public static <T> StreamSource<T> eventSource(
            String name, long eventsPerSecond, long initialDelayMs,
            BiFunctionEx<? super Long, ? super Long, ? extends T> createEventFn
    ) {
        return Sources.streamFromProcessorWithWatermarks(name, true, eventTimePolicy -> ProcessorMetaSupplier.of(
                (Address ignored) -> {
                    long startTime = System.currentTimeMillis() + initialDelayMs;
                    return ProcessorSupplier.of(() ->
                            new EventSourceP(
                                    startTime, eventsPerSecond, eventTimePolicy, createEventFn
                            ));
                })
        );
    }

    public static long simpleTime(long timeMillis) {
        return timeMillis % SIMPLE_TIME_SPAN_MILLIS;
    }

    private static long determineTimeOffset() {
        long milliTime = System.currentTimeMillis();
        long nanoTime = System.nanoTime();
        return NANOSECONDS.toMillis(nanoTime) - milliTime;
    }

    @Override
    protected void init(Context context) {
        name = context.vertexName();
        totalParallelism = context.totalParallelism();
        globalProcessorIndex = context.globalProcessorIndex();
        emitPeriod = SECONDS.toNanos(1) * totalParallelism / itemsPerSecond;
        lastCallNanos = lastReport = emitSchedule =
                startTime + SECONDS.toNanos(1) * globalProcessorIndex / itemsPerSecond;
    }

    @Override
    public boolean complete() {
        nowNanos = System.nanoTime();
        emitEvents();
        detectAndReportHiccup();
        reportThroughput();
        return false;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void emitEvents() {
        if (!emitFromTraverser(traverser)) {
            return;
        }
        if (emitSchedule > nowNanos) {
            maybeEmitWm(nanoTimeToCurrentTimeMillis(nowNanos));
            emitFromTraverser(traverser);
            return;
        }
        do {
            long timestamp = nanoTimeToCurrentTimeMillis(emitSchedule);
            long seq = counter * totalParallelism + globalProcessorIndex;
            Object event = createEventFn.apply(seq, timestamp);
            traverser.append(jetEvent(timestamp, event));
            counter++;
            emitSchedule += emitPeriod;
            maybeEmitWm(timestamp);
        } while (emitFromTraverser(traverser) && emitSchedule <= nowNanos);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void maybeEmitWm(long timestamp) {
        if (timestamp < lastEmittedWm + wmGranularity) {
            return;
        }
        long wmToEmit = timestamp - (timestamp % wmGranularity) + wmOffset;
        long nowMillis = nanoTimeToCurrentTimeMillis(nowNanos);
        long wmLag = nowMillis - wmToEmit;
        if (wmLag > WM_LAG_THRESHOLD_MILLIS) {
            System.out.format("%s#%d: WM is %,d ms behind real time%n", name, globalProcessorIndex, wmLag);
        }
        traverser.append(new Watermark(wmToEmit));
        lastEmittedWm = wmToEmit;
    }

    private void detectAndReportHiccup() {
        long millisSinceLastCall = NANOSECONDS.toMillis(nowNanos - lastCallNanos);
        if (millisSinceLastCall > HICCUP_REPORT_THRESHOLD_MILLIS) {
            System.out.printf("*** %s#%d hiccup: %,d ms%n", name, globalProcessorIndex, millisSinceLastCall);
        }
        lastCallNanos = nowNanos;
    }

    private void reportThroughput() {
        long nanosSinceLastReport = nowNanos - lastReport;
        if (nanosSinceLastReport < THROUGHPUT_REPORT_PERIOD_NANOS) {
            return;
        }
        lastReport = nowNanos;
        long itemCountSinceLastReport = counter - counterAtLastReport;
        counterAtLastReport = counter;
        double throughput = itemCountSinceLastReport / ((double) nanosSinceLastReport / SECONDS.toNanos(1));
        if (throughput >= (double) THROUGHPUT_REPORTING_THRESHOLD) {
            System.out.printf("%,d p%s#%d: %,.0f items/second%n",
                    simpleTime(NANOSECONDS.toMillis(nowNanos)),
                    name,
                    globalProcessorIndex,
                    throughput
            );
        }
    }

    private long nanoTimeToCurrentTimeMillis(long nanoTime) {
        return NANOSECONDS.toMillis(nanoTime) - nanoTimeMillisToCurrentTimeMillis;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        throw new UnsupportedOperationException("Source processor shouldn't be asked to process a watermark");
    }
}

