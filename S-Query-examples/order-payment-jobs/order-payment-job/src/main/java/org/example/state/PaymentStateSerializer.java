package org.example.state;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public class PaymentStateSerializer implements StreamSerializer<PaymentState> {
    @Override
    public int getTypeId() {
        return 6;
    }

    @Override
    public void write(ObjectDataOutput out, PaymentState paymentState) throws IOException {
        out.writeShort(paymentState.getPaymentStatus());
    }

    @Override
    public PaymentState read(ObjectDataInput in) throws IOException {
        short paymentStatus = in.readShort();
        return new PaymentState(paymentStatus);
    }
}