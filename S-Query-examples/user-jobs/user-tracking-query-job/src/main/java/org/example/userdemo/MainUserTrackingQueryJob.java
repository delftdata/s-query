package org.example.userdemo;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import userdemo.SqlHelper;

public class MainUserTrackingQueryJob {
    public static void main(String[] args) {
        JetInstance jet = Jet.bootstrappedInstance();
        SqlHelper.queryGivenMapName("view_counter", "user-tracking", jet, true, false);
    }
}
