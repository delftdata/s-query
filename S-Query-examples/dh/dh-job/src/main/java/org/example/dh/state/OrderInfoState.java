package org.example.dh.state;


import java.time.LocalDateTime;

public class OrderInfoState {
    private double longitudeVendor;
    private double latitudeVendor;
    private double longitudeCustomer;
    private double latitudeCustomer;
    private double longitudeDeliveryZone;
    private double latitudeDeliveryZone;
    private String deliveryZone;
    private String vendorCategory;
    private LocalDateTime promisedDeliveryTimestamp, committedPickupAtTimestamp;

    public OrderInfoState(double longitudeVendor, double latitudeVendor, double longitudeCustomer, double latitudeCustomer, double longitudeDeliveryZone, double latitudeDeliveryZone, String deliveryZone, String vendorCategory, LocalDateTime promisedDeliveryTimestamp, LocalDateTime committedPickupAtTimestamp) {
        this.longitudeVendor = longitudeVendor;
        this.latitudeVendor = latitudeVendor;
        this.longitudeCustomer = longitudeCustomer;
        this.latitudeCustomer = latitudeCustomer;
        this.longitudeDeliveryZone = longitudeDeliveryZone;
        this.latitudeDeliveryZone = latitudeDeliveryZone;
        this.vendorCategory = vendorCategory;
        this.promisedDeliveryTimestamp = promisedDeliveryTimestamp;
        this.committedPickupAtTimestamp = committedPickupAtTimestamp;
        this.deliveryZone = deliveryZone;
    }

    public OrderInfoState() {

    }

    public double getLongitudeVendor() {
        return longitudeVendor;
    }

    public double getLatitudeVendor() {
        return latitudeVendor;
    }

    public double getLongitudeCustomer() {
        return longitudeCustomer;
    }

    public double getLatitudeCustomer() {
        return latitudeCustomer;
    }

    public double getLongitudeDeliveryZone() {
        return longitudeDeliveryZone;
    }

    public double getLatitudeDeliveryZone() {
        return latitudeDeliveryZone;
    }

    public String getDeliveryZone() {
        return deliveryZone;
    }

    public String getVendorCategory() {
        return vendorCategory;
    }

    public LocalDateTime getPromisedDeliveryTimestamp() {
        return promisedDeliveryTimestamp;
    }

    public LocalDateTime getCommittedPickupAtTimestamp() {
        return committedPickupAtTimestamp;
    }

    public void setLongitudeVendor(double longitudeVendor) {
        this.longitudeVendor = longitudeVendor;
    }

    public void setLatitudeVendor(double latitudeVendor) {
        this.latitudeVendor = latitudeVendor;
    }

    public void setLongitudeCustomer(double longitudeCustomer) {
        this.longitudeCustomer = longitudeCustomer;
    }

    public void setLatitudeCustomer(double latitudeCustomer) {
        this.latitudeCustomer = latitudeCustomer;
    }

    public void setLongitudeDeliveryZone(double longitudeDeliveryZone) {
        this.longitudeDeliveryZone = longitudeDeliveryZone;
    }

    public void setLatitudeDeliveryZone(double latitudeDeliveryZone) {
        this.latitudeDeliveryZone = latitudeDeliveryZone;
    }

    public void setDeliveryZone(String deliveryZone) {
        this.deliveryZone = deliveryZone;
    }

    public void setVendorCategory(String vendorCategory) {
        this.vendorCategory = vendorCategory;
    }

    public void setPromisedDeliveryTimestamp(LocalDateTime promisedDeliveryTimestamp) {
        this.promisedDeliveryTimestamp = promisedDeliveryTimestamp;
    }

    public void setCommittedPickupAtTimestamp(LocalDateTime committedPickupAtTimestamp) {
        this.committedPickupAtTimestamp = committedPickupAtTimestamp;
    }
}
