package org.example.dh.events;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public class OrderStatus extends Event {
    private final long orderId;
    private final String orderState;
    private final long updateTimestamp;

    public OrderStatus(long id, long timestamp, long orderId, String orderState, long updateTimestamp) {
        super(id, timestamp);
        this.orderId = orderId;
        this.orderState = orderState;
        this.updateTimestamp = updateTimestamp;
    }

    public long getOrderId() {
        return orderId;
    }

    public String getOrderState() {
        return orderState;
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public static class OrderStatusSerializer implements StreamSerializer<OrderStatus> {

        @Override
        public void write(ObjectDataOutput out, OrderStatus object) throws IOException {
            Event.write(out, object);
            out.writeLong(object.orderId);
            out.writeUTF(object.orderState);
            out.writeLong(object.updateTimestamp);
        }

        @Override
        public OrderStatus read(ObjectDataInput in) throws IOException {
            Tuple2<Long, Long> event = Event.readEvent(in);
            long orderId = in.readLong();
            String orderState = in.readUTF();
            long updateTimestamp = in.readLong();
            return new OrderStatus(event.f0(), event.f1(), orderId, orderState, updateTimestamp);
        }

        @Override
        public int getTypeId() {
            return 3;
        }
    }
}
