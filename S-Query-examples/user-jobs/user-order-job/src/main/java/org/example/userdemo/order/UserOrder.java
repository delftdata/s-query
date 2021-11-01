package org.example.userdemo.order;

import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import userdemo.Category;
import userdemo.UserHelper;

import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class UserOrder implements Serializable {
    private final String userId;
    private final Category category;
    private final int amount;

    public UserOrder(String userId, Category category, int amount) {
        this.userId = userId;
        this.category = category;
        this.amount = amount;
    }

    public static StreamSource<UserOrder> itemStream(
            int itemsPerSecond,
            int numNames,
            int maxAmount
    ) {
        return SourceBuilder.timestampedStream("UserOrderStream",
                ctx -> new UserOrderStreamSource(itemsPerSecond, numNames, maxAmount))
                .fillBufferFn(UserOrderStreamSource::fillBuffer)
                .build();
    }

    public String getUserId() {
        return userId;
    }

    public Category getCategory() {
        return category;
    }

    public int getAmount() {
        return amount;
    }

    private static final class UserOrderStreamSource {
        private static final int MAX_BATCH_SIZE = 1024;

        private final long periodNanos;

        private long emitSchedule;
        private final int numNames;
        private final int maxCategories = Category.values().length;
        private final Category[] categories = Category.values();
        private final int maxAmount;

        private UserOrderStreamSource(int itemsPerSecond, int numNames, int maxAmount) {
            this.numNames = numNames;
            this.maxAmount = maxAmount;
            this.periodNanos = TimeUnit.SECONDS.toNanos(1) / itemsPerSecond;
        }

        void fillBuffer(SourceBuilder.TimestampedSourceBuffer<UserOrder> buf) {
            long nowNs = System.nanoTime();
            if (emitSchedule == 0) {
                emitSchedule = nowNs;
            }
            // round ts down to nearest period
            long tsNanos = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
            long ts = TimeUnit.NANOSECONDS.toMillis(tsNanos - (tsNanos % periodNanos));
            for (int i = 0; i < MAX_BATCH_SIZE && nowNs >= emitSchedule; i++) {
                String userName = UserHelper.generate(numNames);
                int categoryIndex = ThreadLocalRandom.current().nextInt(maxCategories); // Random category (index)
                int amount = ThreadLocalRandom.current().nextInt(1, maxAmount); // Random amount
                UserOrder item = new UserOrder(userName, categories[categoryIndex], amount);
                buf.add(item, ts);
                emitSchedule += periodNanos;
            }
        }
    }
}
