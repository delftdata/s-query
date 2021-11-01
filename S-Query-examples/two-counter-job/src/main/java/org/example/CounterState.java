package org.example;

import java.io.Serializable;

public class CounterState implements Serializable {

    private long counter;

    public CounterState() {
        this(0L);
    }

    public CounterState(long counter) {
        this.counter = counter;
    }

    public long getCounter() {
        return counter;
    }

    public void setCounter(long counter) {
        this.counter = counter;
    }

    public void increaseCounter(long amount) {
        this.counter += amount;
    }
}
