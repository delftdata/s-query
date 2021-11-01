package org.example.events;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public class Payment extends Event {
    public static final class PaymentStatus {
        public static final short PRE_CHECKOUT = 0, CHECKOUT = 1, PAID = 2, REFUNDED = 3, PAYMENT_FAILED = 4;
    }

    private final long orderId;
    private final short paymentStatus;

    public Payment(long id, long timestamp, long orderId, short paymentStatus) {
        super(id, timestamp);
        this.orderId = orderId;
        this.paymentStatus = paymentStatus;
    }


    public long getOrderId() {
        return orderId;
    }

    public short getPaymentStatus() {
        return paymentStatus;
    }

    public static class PaymentSerializer implements StreamSerializer<Payment> {

        @Override
        public int getTypeId() {
            return 3;
        }

        @Override
        public void write(ObjectDataOutput out, Payment order) throws IOException {
            out.writeLong(order.id());
            out.writeLong(order.timestamp());
            out.writeLong(order.getOrderId());
            out.writeShort(order.getPaymentStatus());
        }

        @Override
        public Payment read(ObjectDataInput in) throws IOException {
            long id = in.readLong();
            long timestamp = in.readLong();
            long orderId = in.readLong();
            short paymentStatus = in.readShort();
            return new Payment(id, timestamp, orderId, paymentStatus);
        }
    }
}
