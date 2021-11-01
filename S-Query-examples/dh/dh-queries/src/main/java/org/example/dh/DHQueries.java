package org.example.dh;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import org.example.SqlHelper;

import java.util.Arrays;

public class DHQueries {
    /**
     * Main entry point
     * @param args List of strings:
     *             1. Which query to query (0,1,2,3) (-1 for SELECT *)
     *             2. Limit the results (-1 for all)
     */
    public static void main(String[] args) {
        HazelcastInstance hz = HazelcastClient.newHazelcastClient();

        String[][] queryArgsArray = new String[][]{
                {"COUNT(*), deliveryZone", "(orderState='VENDOR_ACCEPTED' AND lateTimestamp<LOCALTIMESTAMP)", "GROUP BY deliveryZone"},
                {"COUNT(*), vendorCategory", "(orderState='NOTIFIED' OR orderState='ACCEPTED')", "GROUP BY vendorCategory"},
                {"COUNT(*), deliveryZone", "(orderState='VENDOR_ACCEPTED')", "GROUP BY deliveryZone"},
                {"COUNT(*), deliveryZone", "(orderState='PICKED_UP' OR orderState='LEFT_PICKUP' OR orderState='NEAR_CUSTOMER')", "GROUP BY deliveryZone"}
        };

        String[] queryArgs = new String[]{"", "", ""};
        int query = -1;
        if (args.length >= 1) {
            query = Integer.parseInt(args[0]);
            if (query != -1) {
                queryArgs = queryArgsArray[query];
            }
        }
        int limit = -1;
        if (args.length >= 2) {
            limit = Integer.parseInt(args[1]);
            if (limit < 0 && limit != -1) {
                throw new IllegalArgumentException("Query limit should be 0 or higher or -1!");
            }
            if (limit != -1) {
                if (!queryArgs[1].equals("")) {
                    queryArgs[1] += " AND ";
                }
                queryArgs[1] += String.format("CAST(t1.partitionKey AS int) < %d AND CAST(t2.partitionKey AS int) < %d", limit, limit);
            }
        }
        final String[] finalQueryArgs = queryArgs;
        final int finalLimit = limit;
        long[] res;
        if (finalLimit == -1 && query == -1) {
            res = SqlHelper.queryJoinGivenMapNames("orderinfo", "orderstate",
                    "DHBenchmark", "DHBenchmark", hz, true);
        } else {
            res = SqlHelper.queryJoinGivenMapNames("orderinfo", "orderstate",
                    "DHBenchmark", "DHBenchmark", hz, true, finalQueryArgs);
        }
        System.out.println(Arrays.toString(res));
    }
}
