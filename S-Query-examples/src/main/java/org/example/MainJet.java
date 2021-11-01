package org.example;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.impl.processor.TransformStatefulP;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;

import java.util.Collection;
import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;

public class MainJet {
    private static JetInstance jet;
    private static HazelcastInstance hz;
    public static void main(String[] args) {
        // Lambda Runnable
//        Runnable query_task = () -> {
//            try {
//                Thread.sleep(5000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            System.out.println("Query task is running");
//
//            List<String> stateMapNames = hz.getList(TransformStatefulP.STATE_IMAP_NAMES_LIST_NAME);
//            String[] imapsArray = stateMapNames.toArray(new String[0]);
//            System.out.println("List map names:");
//            for (String s : imapsArray) {
//                System.out.println(s);
//            }
//
//            IMap state = hz.getMap(imapsArray[0]);
//            Collection<Object> entries = state.entrySet();
//
//
//            for (Object entry : entries) {
//                System.out.println(entry);
//            }
//
//            while(true) {
//                System.out.println("SQL Query results");
//                try (SqlResult result = hz.getSql().execute(String.format("SELECT * FROM \"%s\"", imapsArray[0]))) {
//                    for (SqlRow row : result) {
//                        Object name = row.getMetadata();
//
//                        System.out.println(name);
//
//                        Long ts = row.getObject("timestamp");
//                        Long[] stateArr = row.getObject("item");
//                        System.out.println(String.format("ts: %d, state: %d", ts, stateArr[0]));
//                    }
//                    System.out.println("Done");
//                } catch (Exception e) {
//
//                }
//
//                PredicateBuilder.EntryObject e = Predicates.newPredicateBuilder().getEntryObject();
//                Predicate predicate = e.get("longarr[0]").greaterEqual(1);
//                Collection<TimestampedItem<Long[]>> greaterOne = state.values(predicate);
//                System.out.println("Predicate result");
//                greaterOne.forEach(ts -> System.out.println(String.format("ts: %d, state: %d", ts.getTimestamp(), ts.getItem()[0])));
//
//                Collection<TimestampedItem<Long[]>> greaterOneSQL = state.values(Predicates.sql("longarr[0] < 1"));
//                System.out.println("Predicate result SQL");
//                greaterOneSQL.forEach(ts -> System.out.println(String.format("ts: %d, state: %d", ts.getTimestamp(), ts.getItem()[0])));
//
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException ex) {
//                    ex.printStackTrace();
//                }
//                System.out.println("Query again");
//            }
//        };

        // start the thread
//        new Thread(query_task).start();

        JobConfig config = new JobConfig();
        config.setName("jet-test");
        Pipeline p = Pipeline.create();
        p.readFrom(TestSources.itemStream(5))
                .withNativeTimestamps(10L).setLocalParallelism(1)
                .groupingKey(e -> e.sequence())
                .mapStateful(
                        SECONDS.toMillis(3),
                        () -> new Long[]{0L},
                        (state, id, event) -> {
                            state[0]++;
                            state[0]++;
                            return state[0];
                        },
                        (state, id, currentWatermark) -> id
                ).setLocalParallelism(1)
                .writeTo(Sinks.logger()).setLocalParallelism(1);
        jet = Jet.bootstrappedInstance();
        hz = jet.getHazelcastInstance();

//        IMap<String, String> stateMapNames = hz.getMap(TransformStatefulP.CUSTOM_ATTRIBUTE_IMAP_NAME);
//        stateMapNames.put("longarr", LongArrayExtractor.class.getName());

        jet.newJob(p, config).join();
    }
}
