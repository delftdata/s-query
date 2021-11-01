package org.example.state;


import java.util.HashMap;

public class OrderState {
    private long size;
    private long total;
    private HashMap<Long, Short> itemCount = new HashMap<>();

    public OrderState() {
        this.size = 0;
        this.total = 0;
    }

    public OrderState(long size, long total, HashMap<Long, Short> itemCount) {
        this.size = size;
        this.total = total;
        this.itemCount = itemCount;
    }

    public long getSize() {
        return size;
    }

    public long getTotal() {
        return total;
    }

    public void addItem(long itemId) {
        itemCount.putIfAbsent(itemId, (short) 0);
        itemCount.computeIfPresent(itemId, (k, c) -> (short)Math.min(c + 1, Short.MAX_VALUE)); // Limit count to short MAX_VALUE
        incrementSize();
        deltaTotal(itemId);
    }

    public boolean removeItem(long itemId) {
        if (!itemCount.containsKey(itemId)) {
            return false;
        }
        itemCount.computeIfPresent(itemId, (k, c) -> c == 1 ? null : (short)(c - 1));
        decrementSize();
        deltaTotal(-itemId);
        return true;
    }

    public void incrementSize() {
        size++;
    }

    public boolean decrementSize() {
        if (size > 0) {
            size--;
        }
        return size > 0;
    }

    public void deltaTotal(long delta) {
        if (total + delta > 0) {
            total += delta;
        }
    }

    public void clear() {
        this.itemCount.clear();
        this.total = 0;
        this.size = 0;
    }

    public HashMap<Long, Short> getItemCount() {
        return itemCount;
    }
}
