package org.example.state;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public class StockStateSerializer implements StreamSerializer<StockState> {
    @Override
    public void write(ObjectDataOutput out, StockState object) throws IOException {
        out.writeLong(object.getStock());
    }

    @Override
    public StockState read(ObjectDataInput in) throws IOException {
        return new StockState(in.readLong());
    }

    @Override
    public int getTypeId() {
        return 7;
    }
}
