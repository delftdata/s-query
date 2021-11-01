package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;

public class OrderPaymentQuery {
    /* Three options:
        - 1 args: 1st = last part of query
        - 2 args: 1st = part inside WHERE clause (without WHERE), 2nd = last part of query
        - 3 args: 1st = part inside SELECT clause (without WHERE), 2nd = part inside WHERE clause (without WHERE), 3rd = last part of query
     */
    public static void main(String[] args) {
        JetInstance jet = Jet.bootstrappedInstance();
        String[] arguments = null;
        if (args.length == 1) {
            arguments = new String[]{"", "", args[0]};
        } else if (args.length == 2) {
            arguments = new String[]{"", args[0], args[1]};
        } else if (args.length == 3) {
            arguments = args;
        } else if (args.length != 0) {
            System.err.println("Invalid amount of arguments, must be either 0, 1, 2, or 3");
            return;
        }
        SqlHelper.queryJoinGivenMapNames("order", "payment",
                "OrderPaymentBenchmark", "OrderPaymentBenchmark", jet.getHazelcastInstance(), true, arguments);
    }
}
