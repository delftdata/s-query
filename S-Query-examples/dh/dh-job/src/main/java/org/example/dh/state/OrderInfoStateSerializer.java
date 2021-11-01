package org.example.dh.state;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class OrderInfoStateSerializer implements StreamSerializer<OrderInfoState> {

    @Override
    public void write(ObjectDataOutput out, OrderInfoState object) throws IOException {
        out.writeDouble(object.getLongitudeVendor());
        out.writeDouble(object.getLatitudeVendor());
        out.writeDouble(object.getLongitudeCustomer());
        out.writeDouble(object.getLatitudeCustomer());
        out.writeDouble(object.getLongitudeDeliveryZone());
        out.writeDouble(object.getLatitudeDeliveryZone());
        out.writeUTF(object.getDeliveryZone());
        out.writeUTF(object.getVendorCategory());
        out.writeLong(object.getPromisedDeliveryTimestamp().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
        out.writeLong(object.getCommittedPickupAtTimestamp().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
    }

    @Override
    public OrderInfoState read(ObjectDataInput in) throws IOException {
        double longitudeVendor = in.readDouble();
        double latitudeVendor = in.readDouble();
        double longitudeCustomer = in.readDouble();
        double latitudeCustomer = in.readDouble();
        double longitudeDeliveryZone = in.readDouble();
        double latitudeDeliveryZone = in.readDouble();
        String deliveryZone = in.readUTF();
        String vendorCategory = in.readUTF();
        LocalDateTime promisedDeliveryTimestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(in.readLong()), ZoneId.systemDefault());
        LocalDateTime committedPickupAtTimestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(in.readLong()), ZoneId.systemDefault());
        return new OrderInfoState(longitudeVendor, latitudeVendor, longitudeCustomer, latitudeCustomer, longitudeDeliveryZone, latitudeDeliveryZone, deliveryZone, vendorCategory, promisedDeliveryTimestamp, committedPickupAtTimestamp);
    }

    @Override
    public int getTypeId() {
        return 5;
    }
}