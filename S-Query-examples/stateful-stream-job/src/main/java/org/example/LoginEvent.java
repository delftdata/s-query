package org.example;

import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;

public class LoginEvent implements Serializable {
    private final long ts;
    private final long id;
    private final LoginEventEnum event;

    public LoginEvent(long ts, long id, LoginEventEnum event) {
        this.ts = ts;
        this.id = id;
        this.event = event;
    }

    public long getTs() {
        return ts;
    }

    public long getId() {
        return id;
    }

    public LoginEventEnum getEvent() {
        return event;
    }

    enum LoginEventEnum {
        LOGIN,
        LOGOUT
    }

    public static StreamSource<LoginEvent> itemStream(
            int itemsPerSecond,
            long numIds,
            @Nonnull LoginEventGeneratorFunction generatorFn
    ) {
        Objects.requireNonNull(generatorFn, "generatorFn");
        checkSerializable(generatorFn, "generatorFn");

        return SourceBuilder.timestampedStream("loginEventStream", ctx -> new LoginEventStreamSource(itemsPerSecond, numIds, generatorFn))
                .fillBufferFn(LoginEventStreamSource::fillBuffer)
                .build();
    }

    @FunctionalInterface
    public interface LoginEventGeneratorFunction extends Serializable {

        /**
         * Applies the function to the given timestamp and sequence.
         *
         * @param timestamp the current timestamp
         * @param numIds the amount of ids to use
         * @param loginEventEnum the login event enum to use
         * @return the function result
         */
        LoginEvent generate(long timestamp, long numIds, LoginEventEnum loginEventEnum);
    }

    private static final class LoginEventStreamSource {
        private static final int MAX_BATCH_SIZE = 1024;

        private final LoginEventGeneratorFunction generator;
        private final long periodNanos;

        private long emitSchedule;
        private final long numIds;

        private LoginEventStreamSource(int itemsPerSecond, long numIds, LoginEventGeneratorFunction generator) {
            this.numIds = numIds;
            this.periodNanos = TimeUnit.SECONDS.toNanos(1) / itemsPerSecond;
            this.generator = generator;
        }

        void fillBuffer(SourceBuilder.TimestampedSourceBuffer<LoginEvent> buf) {
            long nowNs = System.nanoTime();
            if (emitSchedule == 0) {
                emitSchedule = nowNs;
            }
            // round ts down to nearest period
            long tsNanos = TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis());
            long ts = TimeUnit.NANOSECONDS.toMillis(tsNanos - (tsNanos % periodNanos));
            for (int i = 0; i < MAX_BATCH_SIZE && nowNs >= emitSchedule; i++) {
                long id = ThreadLocalRandom.current().nextLong(numIds);
                boolean login = ThreadLocalRandom.current().nextBoolean();
                LoginEventEnum loginEventEnum = login ? LoginEventEnum.LOGIN : LoginEventEnum.LOGOUT;
                LoginEvent item = generator.generate(ts, id, loginEventEnum);
                buf.add(item, ts);
                emitSchedule += periodNanos;
            }
        }
    }
}
