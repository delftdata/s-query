package org.example.state;

public class StockState {
    private long stock;

    public StockState(long stock) {
        this.stock = stock;
    }

    public StockState() {
        this.stock = 0;
    }

    public void deltaStock(long delta) {
        this.stock += delta;
    }

    public long getStock() {
        return stock;
    }
}
