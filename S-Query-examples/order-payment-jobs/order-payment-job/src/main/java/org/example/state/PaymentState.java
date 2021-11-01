package org.example.state;

import static org.example.events.Payment.PaymentStatus.PRE_CHECKOUT;
import static org.example.events.Payment.PaymentStatus.CHECKOUT;
import static org.example.events.Payment.PaymentStatus.PAID;
import static org.example.events.Payment.PaymentStatus.REFUNDED;
import static org.example.events.Payment.PaymentStatus.PAYMENT_FAILED;

public class PaymentState {
    private short paymentStatus;

    public PaymentState() {
        this.paymentStatus = PRE_CHECKOUT;
    }

    public PaymentState(short paymentStatus) {
        this.paymentStatus = paymentStatus;
    }

    public short getPaymentStatus() {
        return paymentStatus;
    }

    /**
     * Set payment status, only valid transitions are
     * PRE_CHECKOUT -> CHECKOUT, CHECKOUT -> PAID, CHECKOUT -> PAYMENT_FAILED, PAID -> REFUNDED.
     * @param paymentStatus Payment status to set
     * @return True if successfully transitioned, false otherwise
     */
    public boolean setPaymentStatus(short paymentStatus) {
        if (this.paymentStatus == PRE_CHECKOUT && paymentStatus == CHECKOUT) {
            this.paymentStatus = CHECKOUT;
            return true;
        }
        if (this.paymentStatus == CHECKOUT && paymentStatus == PAID) {
            boolean fail = Math.random() < 0.20; // 20% chance of failure
            if (fail) {
                this.paymentStatus = PAYMENT_FAILED;
                return false;
            } else {
                this.paymentStatus = PAID;
                return true;
            }
        } else if (this.paymentStatus == PAID && paymentStatus == REFUNDED) {
            this.paymentStatus = REFUNDED;
            return true;
        }
        return false;
    }
}
