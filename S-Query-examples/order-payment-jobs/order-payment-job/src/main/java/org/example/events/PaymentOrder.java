package org.example.events;

import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public class PaymentOrder extends OrderBase {
    private final boolean success; // true -> paid, false -> refund

    public PaymentOrder(long id, long timestamp, long orderId, boolean success) {
        super(id, timestamp, orderId);
        this.success = success;
    }

    /**
     * Getter for success.
     * @return True if order was a success (paid), False if order was a refund
     */
    public boolean isSuccess() {
        return success;
    }

    public static class PaymentOrderSerializer implements StreamSerializer<PaymentOrder> {

        @Override
        public void write(ObjectDataOutput out, PaymentOrder object) throws IOException {
            OrderBase.write(out, object);
            out.writeBoolean(object.success);
        }

        @Override
        public PaymentOrder read(ObjectDataInput in) throws IOException {
            Tuple3<Long, Long, Long> orderBase = OrderBase.readOrderBase(in);
            boolean success = in.readBoolean();
            return new PaymentOrder(orderBase.f0(), orderBase.f1(), orderBase.f2(), success);
        }

        @Override
        public int getTypeId() {
            return 4;
        }
    }
}
