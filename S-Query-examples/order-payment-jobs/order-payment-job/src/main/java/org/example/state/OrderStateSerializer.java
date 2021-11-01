package org.example.state;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class OrderStateSerializer implements StreamSerializer<OrderState> {
    @Override
    public int getTypeId() {
        return 5;
    }

    @Override
    public void write(ObjectDataOutput out, OrderState orderState) throws IOException {
        out.writeLong(orderState.getSize());
        out.writeLong(orderState.getTotal());
        int mapSize = orderState.getItemCount().size();
        out.writeInt(mapSize);
        long[] itemIds = new long[mapSize];
        short[] counters = new short[mapSize];
        int i = 0;
        for (Map.Entry<Long, Short> e : orderState.getItemCount().entrySet()) {
            itemIds[i] = e.getKey();
            counters[i] = e.getValue();
            i++;
        }
        out.writeLongArray(itemIds);
        out.writeShortArray(counters);
    }

    @Override
    public OrderState read(ObjectDataInput in) throws IOException {
        long size = in.readLong();
        long total = in.readLong();
        int mapSize = in.readInt();
        long[] itemIds = in.readLongArray();
        short[] counters = in.readShortArray();
        HashMap<Long, Short> itemCount = new HashMap<>();
        for (int i = 0; i < mapSize; i++) {
            itemCount.put(itemIds[i], counters[i]);
        }
        return new OrderState(size, total, itemCount);
    }
}