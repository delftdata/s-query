package org.example.dh.events;

public class OrderState {
    public static final String ORDER_RECEIVED = "ORDER_RECEIVED";
    public static final String SENT_TO_VENDOR = "SENT_TO_VENDOR";
    public static final String VENDOR_ACCEPTED = "VENDOR_ACCEPTED";
    public static final String NOTIFIED = "NOTIFIED";
    public static final String ACCEPTED = "ACCEPTED";
    public static final String NEAR_VENDOR = "NEAR_VENDOR";
    public static final String PICKED_UP = "PICKED_UP";
    public static final String LEFT_PICKUP = "LEFT_PICKUP";
    public static final String NEAR_CUSTOMER = "NEAR_CUSTOMER";
    public static final String DELIVERED = "DELIVERED";
    public static final String ORDER_COMPLETED = "ORDER_COMPLETED";

    public static final String[] STATES = new String[]{ORDER_RECEIVED, SENT_TO_VENDOR, VENDOR_ACCEPTED, NOTIFIED,
            ACCEPTED, NEAR_VENDOR, PICKED_UP, LEFT_PICKUP, NEAR_CUSTOMER, DELIVERED, ORDER_COMPLETED};
}
