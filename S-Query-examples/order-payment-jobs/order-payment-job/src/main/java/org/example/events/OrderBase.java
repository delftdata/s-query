package org.example.events;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class OrderBase extends Event {
    private final long orderId;

    public OrderBase(long id, long timestamp, long orderId) {
        super(id, timestamp);
        this.orderId = orderId;
    }

    public long getOrderId() {
        return orderId;
    }

   public static void write(ObjectDataOutput out, OrderBase orderBase) throws IOException {
        Event.write(out, orderBase);
        out.writeLong(orderBase.getOrderId());
    }

    public static Tuple3<Long, Long, Long> readOrderBase(ObjectDataInput in) throws IOException {
        Tuple2<Long, Long> event = Event.readEvent(in);
        long orderId = in.readLong();
        return Tuple3.tuple3(event.f0(), event.f1(), orderId);
    }
}
