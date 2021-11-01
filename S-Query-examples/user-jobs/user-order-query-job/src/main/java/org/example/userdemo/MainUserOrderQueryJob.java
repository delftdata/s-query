package org.example.userdemo;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import userdemo.SqlHelper;


public class MainUserOrderQueryJob {
    public static void main(String[] args) {
        JetInstance jet = Jet.bootstrappedInstance();
        SqlHelper.queryGivenMapName("order_counter", "user-orders", jet, true, false);
    }
}
