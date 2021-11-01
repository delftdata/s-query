package org.example.userdemo.tracking;

import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import userdemo.Category;
import userdemo.UserHelper;

import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class UserEvent implements Serializable {
    private final String userName;
    private final Category category;

    public UserEvent(String userName, Category category) {
        this.userName = userName;
        this.category = category;
    }

    public static StreamSource<UserEvent> itemStream(
            int itemsPerSecond,
            int numNames
    ) {
        return SourceBuilder.timestampedStream("UserEventStream",
                ctx -> new LoginEventStreamSource(itemsPerSecond, numNames))
                .fillBufferFn(LoginEventStreamSource::fillBuffer)
                .build();
    }

    public String getUserName() {
        return userName;
    }

    public Category getCategory() {
        return category;
    }

    private static final class LoginEventStreamSource {
        private static final int MAX_BATCH_SIZE = 1024;

        private final long periodNanos;

        private long emitSchedule;
        private final int numNames;
        private final int maxCategories = Category.values().length;
        private final Category[] categories = Category.values();

        private LoginEventStreamSource(int itemsPerSecond, int numNames) {
            this.numNames = numNames;
            this.periodNanos = TimeUnit.SECONDS.toNanos(1) / itemsPerSecond;
        }

        void fillBuffer(SourceBuilder.TimestampedSourceBuffer<UserEvent> buf) {
            long nowNs = System.nanoTime();
            if (emitSchedule == 0) {
                emitSchedule = nowNs;
            }
            // round ts down to nearest period
            long tsNanos = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
            long ts = TimeUnit.NANOSECONDS.toMillis(tsNanos - (tsNanos % periodNanos));
            for (int i = 0; i < MAX_BATCH_SIZE && nowNs >= emitSchedule; i++) {
                String userName = UserHelper.generate(numNames); // Random user name
                int categoryIndex = ThreadLocalRandom.current().nextInt(maxCategories); // Random category (index)
                UserEvent item = new UserEvent(userName, categories[categoryIndex]);
                buf.add(item, ts);
                emitSchedule += periodNanos;
            }
        }
    }
}
