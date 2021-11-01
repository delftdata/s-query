package org.example.dh.events;

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public class RiderLocation extends Event {
    private final long orderId;
    private final long updateTimestamp;
    private final double longitude, latitude;

    public RiderLocation(long id, long timestamp, long orderId, long updateTimestamp, double longitude, double latitude) {
        super(id, timestamp);
        this.orderId = orderId;
        this.updateTimestamp = updateTimestamp;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public long getOrderId() {
        return orderId;
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public static class RiderLocationSerializer implements StreamSerializer<RiderLocation> {

        @Override
        public void write(ObjectDataOutput out, RiderLocation object) throws IOException {
            out.writeLong(object.id());
            out.writeLong(object.timestamp());
            out.writeLong(object.orderId);
            out.writeLong(object.updateTimestamp);
            out.writeDouble(object.longitude);
            out.writeDouble(object.latitude);
        }

        @Override
        public RiderLocation read(ObjectDataInput in) throws IOException {
            Tuple2<Long, Long> event = Event.readEvent(in);
            long orderId = in.readLong();
            long updateTimestamp = in.readLong();
            double longitude = in.readDouble();
            double latitude = in.readDouble();
            return new RiderLocation(event.f0(), event.f1(), orderId, updateTimestamp, longitude, latitude);
        }

        @Override
        public int getTypeId() {
            return 2;
        }
    }
}
