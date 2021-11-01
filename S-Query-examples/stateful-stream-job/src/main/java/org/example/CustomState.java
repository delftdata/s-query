package org.example;

import java.io.Serializable;

public class CustomState implements Serializable {
    private Long longState = 0L;
    private String stringState = "";

    public Long getLongState() {
        return longState;
    }

    public void setLongState(Long longState) {
        this.longState = longState;
    }

    public String getStringState() {
        return stringState;
    }

    public void setStringState(String stringState) {
        this.stringState = stringState;
    }
}
