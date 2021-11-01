package com.hazelcast.map.impl.proxy;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.impl.processor.SnapshotIMapKey;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.map.impl.proxy.MapProxySupport.NULL_KEY_IS_NOT_ALLOWED;
import static com.hazelcast.map.impl.proxy.MapProxySupport.NULL_VALUE_IS_NOT_ALLOWED;

public class SampleImapsJob {
    private static final long NUM_EMPLOYEES = 100_000L;
    private static long start = 0;
    private static long end = 0;
    private static IMap<Long, Object> sample;
    private static IMap<Object, Object> sampleData;
    private static IMap<Object, Object> sampleKeyData;

    private static IMap<Long, Object> custom;
    private static IMap<Object, Object> customData;
    private static IMap<Object, Object> customKeyData;

    private static HazelcastInstanceProxy hz;

    private static final Map<Long, Employee> tempMap = new HashMap<>();
    private static final Map<Long, Data> tempDataMap = new HashMap<>();
    private static final Map<Data, Data> tempKeyDataMap = new HashMap<>();

    private static final List<Employee> employees = new ArrayList<>();
    private static final List<Data> employeesData = new ArrayList<>();

    private static final AtomicBoolean stop = new AtomicBoolean(false);

    private static MapEntries[] entries;
    private static MapEntries[] entriesData;
    private static MapEntries[] entriesKeyData;

    private static long ageCounter = 0;

    private static void putHash() {
        ageCounter++;
        Employee e = new Employee("name"+ageCounter, ageCounter);
        start = System.nanoTime();
        tempMap.put(ageCounter, e);
        end = System.nanoTime();
        System.out.printf("put time: %d%n", end - start);
    }

    private static void evictHash() {
        start = System.nanoTime();
        sample.evict(ageCounter);
        end = System.nanoTime();
        System.out.printf("Evict time: %d%n", end - start);
    }

    private static void set() {
        ageCounter++;
        Employee e = new Employee("name"+ageCounter, ageCounter);
        start = System.nanoTime();
        sample.set(ageCounter, e);
        end = System.nanoTime();
        System.out.printf("set time: %d%n", end - start);
    }

    public static void setAll() {
        start = System.nanoTime();
        sample.setAll(tempMap);
        end = System.nanoTime();
        System.out.printf("setAll time: %d%n", end - start);
    }

    public static void putDataHash() {
        ageCounter++;
        Employee e = new Employee("name"+ageCounter, ageCounter);
        Data eData = hz.getSerializationService().toData(e);
        start = System.nanoTime();
        tempDataMap.put(ageCounter, eData);
        end = System.nanoTime();
        System.out.printf("put data time: %d%n", end - start);
    }

    public static void setData() {
        ageCounter++;
        Employee e = new Employee("name"+ageCounter, ageCounter);
        Data eData = hz.getSerializationService().toData(e);
        start = System.nanoTime();
        sampleData.set(ageCounter, eData);
        end = System.nanoTime();
        System.out.printf("set data time: %d%n", end - start);
    }

    public static void setAllData() {
        start = System.nanoTime();
        sampleData.setAll(tempDataMap);
        end = System.nanoTime();
        System.out.printf("setAll data time: %d%n", end - start);
    }

    public static void setAllKeyData() {
        start = System.nanoTime();
        sampleKeyData.setAll(tempKeyDataMap);
        end = System.nanoTime();
        System.out.printf("setAll key data time: %d%n", end - start);
    }

    public static void setAllCustom() {
        start = System.nanoTime();
        setAllFast((MapProxyImpl) custom, entries);
        end = System.nanoTime();
        System.out.printf("setAll custom time: %d%n", end - start);
    }

    public static void setAllCustomData() {
        start = System.nanoTime();
        setAllFast((MapProxyImpl) customData, entriesData);
        end = System.nanoTime();
        System.out.printf("setAll custom data time: %d%n", end - start);
    }

    public static void setAllCustomKeyData() {
        start = System.nanoTime();
        setAllFast((MapProxyImpl) customKeyData, entriesKeyData);
        end = System.nanoTime();
        System.out.printf("setAll custom key data time: %d%n", end - start);
    }

    public static void evictAll() {
        start = System.nanoTime();
        custom.evictAll();
        end = System.nanoTime();
        System.out.printf("evict all custom time: %d%n", end - start);
    }

    public static void waitSome() throws InterruptedException {
//        Thread.sleep(100);
        populateData();
    }

    public static void setAllFast(MapProxyImpl imap, MapEntries[] entriesPerPartition) {
        Map<Address, List<Integer>> memberPartitionsMap = imap.partitionService.getMemberPartitionsMap();
        // invoke operations for entriesPerPartition
        AtomicInteger counter = new AtomicInteger(memberPartitionsMap.size());
        InternalCompletableFuture<Void> resultFuture = new InternalCompletableFuture<>();
        BiConsumer<Void, Throwable> callback = (response, t) -> {
            if (t != null) {
                resultFuture.completeExceptionally(t);
            }
            if (counter.decrementAndGet() == 0) {
//                finalizePutAll(map);
                if (!resultFuture.isDone()) {
                    resultFuture.complete(null);
                }
            }
        };
        for (Map.Entry<Address, List<Integer>> entry : memberPartitionsMap.entrySet()) {
            imap.invokePutAllOperation(entry.getKey(), entry.getValue(), entriesPerPartition, false)
                    .whenCompleteAsync(callback);
        }
        // if executing in sync mode, block for the responses
        try {
            resultFuture.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static MapEntries[] mapToEntries(Map<?, ?> map, MapProxyImpl<?, ?> imap) {
        // fill entriesPerPartition
        int mapSize = map.size();
        int partitionCount = imap.partitionService.getPartitionCount();
        MapEntries[] entriesPerPartition = new MapEntries[partitionCount];
        for (Map.Entry entry : map.entrySet()) {
            checkNotNull(entry.getKey(), NULL_KEY_IS_NOT_ALLOWED);
            checkNotNull(entry.getValue(), NULL_VALUE_IS_NOT_ALLOWED);

            Data keyData = hz.getSerializationService().toData(entry.getKey(), imap.partitionStrategy);
            int partitionId = imap.partitionService.getPartitionId(keyData);
            MapEntries entries = entriesPerPartition[partitionId];
            if (entries == null) {
                int initialSize = imap.getPutAllInitialSize(false, mapSize, partitionCount);
                entries = new MapEntries(initialSize);
                entriesPerPartition[partitionId] = entries;
            }

            entries.add(keyData, hz.getSerializationService().toData(entry.getValue()));
        }
        return entriesPerPartition;
    }

    private static void populateData() {
        for (long i = 0; i < NUM_EMPLOYEES; i++) {
            Employee e = new Employee("name" + i, ageCounter+i);
            Data eData = hz.getSerializationService().toData(e);
            Data kData = hz.getSerializationService().toData(i);
            employees.add(e);
            employeesData.add(eData);
            tempMap.put(i, e);
            tempDataMap.put(i, eData);
            tempKeyDataMap.put(kData, eData);
        }
        entries = mapToEntries(tempMap, (MapProxyImpl<Long, Object>) custom);
        entriesData = mapToEntries(tempDataMap, (MapProxyImpl<Object, Object>) customData);
        entriesKeyData = mapToEntries(tempKeyDataMap, (MapProxyImpl<Object, Object>) customKeyData);
        ageCounter++;
    }

    public static void main(String[] args) {
        SerializerConfig sc = new SerializerConfig()
                .setImplementation(new EmployeeSerializer())
                .setTypeClass(Employee.class);
        Config config = new Config("test");
        config.getSerializationConfig().addSerializerConfig(sc);
        hz = (HazelcastInstanceProxy) Hazelcast.getOrCreateHazelcastInstance(config);
        sample = hz.getMap("sample");
        sampleData = hz.getMap("sampledata");
        sampleKeyData = hz.getMap("samplekeydata");
        custom = hz.getMap("custom");
        customData = hz.getMap("customdata");
        customKeyData = hz.getMap("customkeydata");

        populateData();

//        SnapshotIMapKey<Employee> snapshotIMapKey = new SnapshotIMapKey<>(employees.get(0), 1);
//        Data d = hz.getSerializationService().toData(snapshotIMapKey);
//        Data d2 = SnapshotIMapKey.fromData(employeesData.get(0), 1);
//
//        byte[] arr = d.toByteArray();
//        byte[] arr2 = d2.toByteArray();
//
//        System.out.printf("Arr 1 length: %d%n", arr.length);
//        System.out.printf("Arr 2 length: %d%n", arr2.length);
//
//        System.out.println(Arrays.toString(arr));
//        System.out.println(Arrays.toString(arr2));
//        System.out.printf("Partition hash: %d%n", d.getPartitionHash());
//        System.out.printf("Partition hash arr: %d%n", Bits.readInt(arr, 0, true));
//        System.out.printf("Partition hash 2: %d%n", d2.getPartitionHash());
//        System.out.printf("Partition hash arr 2: %d%n", Bits.readInt(arr2, 0, true));
//
//        System.out.printf("Type ID: %d%n", d.getType());
//        System.out.printf("Type ID arr: %d%n", Bits.readInt(arr, 4, true));
//        System.out.printf("Type ID 2: %d%n", d2.getType());
//        System.out.printf("Type ID arr 2: %d%n", Bits.readInt(arr2, 4, true));
//
//        byte[] employeeArr = Arrays.copyOfRange(arr, 8, arr.length - 8);
//        byte[] employeeArr2 = Arrays.copyOfRange(arr2, 8, arr2.length - 8);
//        System.out.println(Arrays.toString(employeeArr));
//        System.out.println(Arrays.toString(employeeArr2));
////        hz.getSerializationService().readObject()
//        System.out.printf("Snapshot ID: %d%n", Bits.readLong(arr, arr.length - 8, true));
//        System.out.printf("Snapshot ID: %d%n", Bits.readLong(arr2, arr2.length - 8, true));


        while(!stop.get()) {
            try {
//                putHash();
//                waitSome();
//
//                putDataHash();
//                waitSome();
//
//                set();
//                waitSome();
//
//                setData();
//                waitSome();
//
//                setAll();
//                waitSome();
//
//                setAllData();
//                waitSome();
//
//                setAllKeyData();
//                waitSome();

                setAllCustom();
                waitSome();

//                setAllCustomData();
//                waitSome();
//
//                setAllCustomKeyData();
//                waitSome();
//
//                evictHash();
//                waitSome();

                evictAll();
                waitSome();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//            stop.set(true);
        }



//        sampleone.put(0L, e0);
//        sampleone.put(1L, new Employee("name2", 40));
//        sampleone.put(2L, new Employee("name3", 50));
//
//        sampletwo.put(0L, new Employee("name1", 31));
//        sampletwo.put(1L, new Employee("name2", 41));
//        sampletwo.put(2L, new Employee("name3", 51));

//      SELECT "snapshot-id1-map".partitionKey, "snapshot-id1-map".counter, "snapshot-id2-map".partitionKey, "snapshot-id2-map".counter FROM "snapshot-id1-map" JOIN "snapshot-id2-map" ON "snapshot-id1-map".partitionKey="snapshot-id2-map".partitionKey;
//      SELECT "snapshot-id1-map".partitionKey AS key1, "snapshot-id1-map".counter AS counter1, "snapshot-id2-map".partitionKey AS key2, "snapshot-id2-map".counter AS counter2, ("snapshot-id1-map".counter + "snapshot-id2-map".counter) AS combined FROM "snapshot-id1-map" INNER JOIN "snapshot-id2-map" USING (partitionKey);

        try (SqlResult result = hz.getSql().execute(
                "SELECT customdata.name, customkeydata.name, customdata.age, customkeydata.age, sample.name, sample.age AS a3, (customdata.age+customkeydata.age+sample.age) AS ageSum FROM customdata JOIN customkeydata USING(name) JOIN sample ON customdata.name=sample.name")) {
            for (SqlRow row : result) {
                String name1 = row.getObject(0);
                String name2 = row.getObject(1);
                String name3 = row.getObject(4);
                long age1 = row.getObject(2);
                long age2 = row.getObject(3);
                long age3 = row.getObject(5);
                long ageSum = row.getObject(6);

                System.out.printf("%s, %s, %s, %d, %d, %d, %d%n", name1, name2, name3, age1, age2, age3, ageSum);
            }
        }
    }

}
