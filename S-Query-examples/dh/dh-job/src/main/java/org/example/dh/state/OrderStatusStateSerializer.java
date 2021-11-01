package org.example.dh.state;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class OrderStatusStateSerializer implements StreamSerializer<OrderStatusState> {
    @Override
    public void write(ObjectDataOutput out, OrderStatusState object) throws IOException {
        out.writeUTF(object.getOrderState());
        out.writeLong(object.getUpdateTimestamp().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
    }

    @Override
    public OrderStatusState read(ObjectDataInput in) throws IOException {
        return new OrderStatusState(in.readUTF(), LocalDateTime.ofInstant(Instant.ofEpochMilli(in.readLong()), ZoneId.systemDefault()));
    }

    @Override
    public int getTypeId() {
        return 7;
    }
}
