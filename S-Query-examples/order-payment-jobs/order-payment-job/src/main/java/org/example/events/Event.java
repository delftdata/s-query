package org.example.events;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class Event {
    private final long id;
    private final long timestamp;

    public Event(long id, long timestamp) {
        this.id = id;
        this.timestamp = timestamp;
    }

    public long id() {
        return id;
    }

    public long timestamp() {
        return timestamp;
    }

    public static void write(ObjectDataOutput out, Event e) throws IOException {
        out.writeLong(e.id);
        out.writeLong(e.timestamp);
    }

    public static Tuple2<Long, Long> readEvent(ObjectDataInput in) throws IOException {
        long id = in.readLong();
        long timestamp = in.readLong();
        return Tuple2.tuple2(id, timestamp);
    }
}
