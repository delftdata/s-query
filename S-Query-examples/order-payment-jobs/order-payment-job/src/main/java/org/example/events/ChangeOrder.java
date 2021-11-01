package org.example.events;

import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public class ChangeOrder extends OrderBase {
    private final long itemId;
    private final boolean operation;

    public ChangeOrder(long id, long timestamp, long orderId, long itemId, boolean operation) {
        super(id, timestamp, orderId);
        this.itemId = itemId;
        this.operation = operation;
    }

    public long getItemId() {
        return itemId;
    }

    public boolean getOperation() {
        return operation;
    }

    public static class ChangeOrderSerializer implements StreamSerializer<ChangeOrder> {

        @Override
        public void write(ObjectDataOutput out, ChangeOrder object) throws IOException {
            OrderBase.write(out, object);
            out.writeLong(object.itemId);
            out.writeBoolean(object.operation);
        }

        @Override
        public ChangeOrder read(ObjectDataInput in) throws IOException {
            Tuple3<Long, Long, Long> orderBase = OrderBase.readOrderBase(in);
            long itemId = in.readLong();
            boolean operation = in.readBoolean();
            return new ChangeOrder(orderBase.f0(), orderBase.f1(), orderBase.f2(), itemId, operation);
        }

        @Override
        public int getTypeId() {
            return 1;
        }
    }
}
