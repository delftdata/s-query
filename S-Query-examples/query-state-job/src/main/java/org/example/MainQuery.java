package org.example;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.impl.processor.IMapStateHelper;
import com.hazelcast.jet.impl.processor.TransformStatefulP;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class MainQuery {
    public static void main(String[] args) {
        JetInstance jet = Jet.bootstrappedInstance();
        HazelcastInstance hz = jet.getHazelcastInstance();
        final AtomicBoolean stop = new AtomicBoolean(false);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> stop.set(true)));

        if (args.length == 1) {
            String queryString = String.format("SELECT * FROM \"%1$s\"", args[0]);
            try (SqlResult result = hz.getSql().execute(queryString)) {
                for (SqlRow row : result) {
                    System.out.println(row.getObject(0).toString());
                }
            }
            return;
        }

        if (args.length == 2) {
            String queryString = String.format("SELECT * FROM \"%1$s\"", args[0]);
            int sleepTime = Integer.parseInt(args[1]);
            while (!stop.get()) {
                try (SqlResult result = hz.getSql().execute(queryString)) {
//                    for (SqlRow row : result) {
//                        System.out.println(row.getObject(0).toString());
//                    }
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    stop.set(true);
                    e.printStackTrace();
                }
            }
            return;
        }

        String ssMap1 = IMapStateHelper.getSnapshotMapName("id1-map");
        String ssMap2 = IMapStateHelper.getSnapshotMapName("id2-map");

        System.out.println(ssMap1);
        System.out.println(ssMap2);
        String queryString = String.format("SELECT \"%1$s\".partitionKey AS key1, \"%1$s\".counter AS counter1, \"%2$s\".partitionKey AS key2, \"%2$s\".counter AS counter2, (\"%1$s\".counter + \"%2$s\".counter) AS combined, \"%1$s\".snapshotId AS ss1, \"%2$s\".snapshotId AS ss2 FROM \"%1$s\" INNER JOIN \"%2$s\" USING (partitionKey, snapshotId)", ssMap1, ssMap2);
        System.out.println(queryString);

        try (SqlResult result = hz.getSql().execute(queryString)) {
            System.out.println("Key1, Key2, Counter1, Counter2, Countertotal, Snapshot 1, Snapshot 2");
            for (SqlRow row : result) {
                long key1 = row.getObject(0);
                long counter1 = row.getObject(1);
                long key2 = row.getObject(2);
                long counter2 = row.getObject(3);
                long countertotal = row.getObject(4);
                long ss1 = row.getObject(5);
                long ss2 = row.getObject(6);

                System.out.printf("%d,\t%d,\t%d,\t%d,\t%d,\t%d,\t%d%n", key1, key2, counter1, counter2, countertotal, ss1, ss2);
            }
        }

//        String[] imapsArray = stateMapNames.toArray(new String[0]);
//        String[] ssArray = snapshotMapNames.toArray(new String[0]);
//        System.out.println("State IMaps:");
//        for (String s : imapsArray) {
//            System.out.println("\t" + s);
//        }
//        System.out.println("Snapshot IMaps:");
//        for (String s : ssArray) {
//            System.out.println("\t" + s);
//        }

//        for (String stateIMap : imapsArray) {
//            System.out.println("\nResults for state IMap: " + stateIMap);
//            System.out.println("\tSQL Query results:");
//            try (SqlResult result = hz.getSql().execute(String.format("SELECT * FROM \"%s\"", stateIMap))) {
//                for (SqlRow row : result) {
//                    Object metadata = row.getMetadata();
//                    System.out.println("\t\t" + metadata);
//                    Long ts = row.getObject("timestamp");
//                    Long[] stateObj = row.getObject("item");
//                    System.out.println(String.format("\t\tts: %d, state: %d", ts, stateObj[0]));
//                }
//            } catch (Exception e) {
//
//            }
//
//            IMap state = hz.getMap(imapsArray[0]);
//
//            PredicateBuilder.EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
//            Predicate predicate = e.get("longarr[0]").greaterEqual(1);
//            Collection<TimestampedItem<Long[]>> greaterOne = state.values(predicate);
//            System.out.println("\tPredicate result:");
//            greaterOne.forEach(ts -> System.out.println(
//                    String.format("\t\tts: %d, state: %d", ts.timestamp(), ts.item()[0])));
//
//            String queryGreaterOne = "longarr[0] >= 1";
//            Collection<TimestampedItem<Long[]>> greaterOneSQL = state.values(Predicates.sql(queryGreaterOne));
//            System.out.println(String.format("\tPredicate result using SQL string: '%s'", queryGreaterOne));
//            greaterOneSQL.forEach(ts -> System.out.println(
//                    String.format("\t\tts: %d, state: %d", ts.timestamp(), ts.item()[0])));
//        }
//
//        for (String ssIMap : ssArray) {
//            System.out.println("\nResults for state IMap: " + ssIMap);
//            System.out.println("\tSQL Query results:");
//            try (SqlResult result = hz.getSql().execute(String.format("SELECT * FROM \"%s\"", ssIMap))) {
//                for (SqlRow row : result) {
//                    Object metadata = row.getMetadata();
//                    System.out.println("\t\t" + metadata);
//                    Long ts = row.getObject("timestamp");
//                    Long[] stateObj = row.getObject("item");
//                    System.out.println(String.format("\t\tts: %d, state: %d", ts, stateObj[0]));
//                }
//            } catch (Exception e) {
//
//            }
//
//            IMap state = hz.getMap(imapsArray[0]);
//
//            PredicateBuilder.EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
//            Predicate predicate = e.get("longarr[0]").greaterEqual(1);
//            Collection<TimestampedItem<Long[]>> greaterOne = state.values(predicate);
//            System.out.println("\tPredicate result:");
//            greaterOne.forEach(ts -> System.out.println(
//                    String.format("\t\tts: %d, state: %d", ts.timestamp(), ts.item()[0])));
//
//            String queryGreaterOne = "longarr[0] >= 1";
//            Collection<TimestampedItem<Long[]>> greaterOneSQL = state.values(Predicates.sql(queryGreaterOne));
//            System.out.println(String.format("\tPredicate result using SQL string: '%s'", queryGreaterOne));
//            greaterOneSQL.forEach(ts -> System.out.println(
//                    String.format("\t\tts: %d, state: %d", ts.timestamp(), ts.item()[0])));
//        }
    }
}
