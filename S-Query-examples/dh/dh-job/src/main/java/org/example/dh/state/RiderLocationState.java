package org.example.dh.state;

import java.time.LocalDateTime;

public class RiderLocationState {
    private LocalDateTime updateTimestamp;
    private double longitude;
    private double latitude;

    public RiderLocationState(LocalDateTime updateTimestamp, double longitude, double latitude) {
        this.updateTimestamp = updateTimestamp;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public RiderLocationState() {
        this.updateTimestamp = LocalDateTime.now(); // Prevent nullpointer exception on serialization
    }

    public LocalDateTime getUpdateTimestamp() {
        return updateTimestamp;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setUpdateTimestamp(LocalDateTime updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }
}
