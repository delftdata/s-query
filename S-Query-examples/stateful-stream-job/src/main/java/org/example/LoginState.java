package org.example;

import java.io.Serializable;

public class LoginState implements Serializable {
    private int state;

    public LoginState(int state) {
        this.state = state;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    enum LoginStateEnum {
        LOGGED_IN,
        LOGGED_OUT
    }
}
