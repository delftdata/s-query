package org.example;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;

import java.util.Arrays;

public class GenericQuery {
    /*
    - First four args are always:
        1. transform 1 name
        2. transform 2 name
        3. transform 1 job name
        4. transform 2 job name
    After this four options:
        - 4 args: No extra args, this will do a SELECT *
        - 5 args: 5th = last part of query
        - 6 args: 5th = part inside WHERE clause (without WHERE), 6th = last part of query
        - 7 args: 5th = part inside SELECT clause (without WHERE), 6th = part inside WHERE clause (without WHERE), 7th = last part of query
     */
    public static void main(String[] args) {
        JetInstance jet = Jet.bootstrappedInstance();
        String[] arguments = null;
        final int minimumArgs = 4;
        if (args.length == minimumArgs + 1) {
            arguments = new String[]{"", "", args[minimumArgs]};
        } else if (args.length == minimumArgs + 2) {
            arguments = new String[]{"", args[minimumArgs], args[minimumArgs + 1]};
        } else if (args.length == minimumArgs + 3) {
            arguments = Arrays.copyOfRange(args, minimumArgs, minimumArgs + 3);
        } else if (args.length != minimumArgs) {
            System.err.println("Invalid amount of arguments, must be either 4, 5, 6, or 7");
            return;
        }
        SqlHelper.queryJoinGivenMapNames(args[0], args[1], args[2], args[3], jet.getHazelcastInstance(), true, arguments);
    }
}
