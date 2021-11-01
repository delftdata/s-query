package org.example.events;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public class ChangeStock extends Event {

    private final long itemId;
    private final short stockDelta;

    public ChangeStock(long id, long timestamp, long itemId, short stockDelta) {
        super(id, timestamp);
        this.itemId = itemId;
        this.stockDelta = stockDelta;
    }

    public long getItemId() {
        return itemId;
    }

    public long getStockDelta() {
        return stockDelta;
    }

    public static class ChangeStockSerializer implements StreamSerializer<ChangeStock> {

        @Override
        public void write(ObjectDataOutput out, ChangeStock object) throws IOException {
            out.writeLong(object.id());
            out.writeLong(object.timestamp());
            out.writeLong(object.itemId);
            out.writeShort(object.stockDelta);
        }

        @Override
        public ChangeStock read(ObjectDataInput in) throws IOException {
            Tuple2<Long, Long> event = Event.readEvent(in);
            long itemId = in.readLong();
            short stockDelta = in.readShort();
            return new ChangeStock(event.f0(), event.f1(), itemId, stockDelta);
        }

        @Override
        public int getTypeId() {
            return 2;
        }
    }
}
