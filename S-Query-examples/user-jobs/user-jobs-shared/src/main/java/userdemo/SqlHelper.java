package userdemo;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.impl.processor.IMapStateHelper;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class SqlHelper {
    private static final String PARTITION_KEY = "partitionKey";
    private static final String SNAPSHOT_ID = "snapshotId";
    private static final int DEFAULT_EXTRA_SPACING = 2;

    private static class DistributedObjectNames {
        private final String liveMapName;
        private final String snapshotMapName;
        private final String snapshotIdName;

        public DistributedObjectNames(String liveMapName, String snapshotMapName, String snapshotIdName) {
            this.liveMapName = liveMapName;
            this.snapshotMapName = snapshotMapName;
            this.snapshotIdName = snapshotIdName;
        }

        public String getLiveMapName() {
            return liveMapName;
        }

        public String getSnapshotMapName() {
            return snapshotMapName;
        }

        public String getSnapshotIdName() {
            return snapshotIdName;
        }
    }

    private static DistributedObjectNames getDistObjectNames(String transformName, String jobName, HazelcastInstance hz) {
        String liveMapName = IMapStateHelper.getPhaseSnapshotMapName(transformName);
        String ssMapName = IMapStateHelper.getPhaseSnapshotMapName(transformName);
        String ssIdName = IMapStateHelper.getSnapshotIdName(jobName);

        return new DistributedObjectNames(liveMapName, ssMapName, ssIdName);
    }

    private static long getQueryableSnapshotId(String ssIdName, HazelcastInstance hz, String transformName, String jobName) {
        IAtomicLong distributedSnapshotId = hz.getCPSubsystem().getAtomicLong(ssIdName);
//        ICountDownLatch clusterCountDownLatch = hz.getCPSubsystem().getCountDownLatch(IMapStateHelper.clusterCountdownLatchHelper(jobName));
        long snapshotId = distributedSnapshotId.get();
//        int clusterCountDownLatchState = clusterCountDownLatch.getCount();
        // Use the latest snapshot id - 1 by default
        long querySnapshotId = Math.max(0, snapshotId);
//        if (clusterCountDownLatchState == 0) {
//            // In case the latest snapshot is finished, use the latest id
//            querySnapshotId = Math.max(0, snapshotId);
//        }
        System.out.printf("Latest snapshot id for %s: %d, querying: %d%n", transformName, snapshotId, querySnapshotId);
        return querySnapshotId;
    }

    public static void queryGivenMapName(String transformName, String jobName, JetInstance jet) {
        queryGivenMapName(transformName, jobName, jet, true, true);
    }

    public static void queryGivenMapName(String transformName, String jobName, JetInstance jet, boolean querySs, boolean printType) {
        HazelcastInstance hz = jet.getHazelcastInstance();

        DistributedObjectNames distributedObjectNames = getDistObjectNames(transformName, jobName, hz);
        String liveMapName = distributedObjectNames.getLiveMapName();
        String ssMapName = distributedObjectNames.getSnapshotMapName();
        String ssIdName = distributedObjectNames.getSnapshotIdName();

        long querySnapshotId = getQueryableSnapshotId(ssIdName, hz, transformName, jobName);

        String queryMap = querySs ? ssMapName : liveMapName;
        String queryString = String.format("SELECT * FROM \"%s\" WHERE snapshotId=%d", queryMap, querySnapshotId);
        System.out.println(queryString);

        try (SqlResult result = hz.getSql().execute(queryString)) {
            resultToHeaderAndRows(result, printType).forEach(System.out::println);
        }
    }

    public static void queryJoinGivenMapNames(String transformName1, String transformName2, String jobName1, String jobName2, JetInstance jet) {
        queryJoinGivenMapNames(transformName1, transformName2, jobName1, jobName2, jet, true);
    }

    public static void queryJoinGivenMapNames(String transformName1, String transformName2, String jobName1, String jobName2, JetInstance jet, boolean querySs) {
        HazelcastInstance hz = jet.getHazelcastInstance();
        DistributedObjectNames distributedObjectNames1 = getDistObjectNames(transformName1, jobName1, hz);
        String liveMapName1 = distributedObjectNames1.getLiveMapName();
        String ssMapName1 = distributedObjectNames1.getSnapshotMapName();
        String ssIdName1 = distributedObjectNames1.getSnapshotIdName();
        long querySnapshotId1 = getQueryableSnapshotId(ssIdName1, hz, transformName1, jobName1);
        DistributedObjectNames distributedObjectNames2 = getDistObjectNames(transformName2, jobName2, hz);
        String liveMapName2 = distributedObjectNames2.getLiveMapName();
        String ssMapName2 = distributedObjectNames2.getSnapshotMapName();
        String ssIdName2 = distributedObjectNames2.getSnapshotIdName();
        long querySnapshotId2 = getQueryableSnapshotId(ssIdName2, hz, transformName2, jobName2);


        String queryMap1 = querySs ? ssMapName1 : liveMapName1;
        String queryMap2 = querySs ? ssMapName2 : liveMapName2;

        String queryString = MessageFormat.format(
                "SELECT t1.*, t2.* FROM \"{0}\" t1 JOIN \"{1}\" t2 USING({4}) WHERE t1.{5}={2,number,#} AND t2.{5}={3,number,#}",
                queryMap1,
                queryMap2,
                querySnapshotId1,
                querySnapshotId2,
                PARTITION_KEY,
                SNAPSHOT_ID
        );
        System.out.println(queryString);

        try (SqlResult result = hz.getSql().execute(queryString)) {
            resultToHeaderAndRows(result, false).forEach(System.out::println);
        }
    }

    private static String getNChars(int n, char character) {
        StringBuilder sb = new StringBuilder(n);
        for (int i=0; i < n; i++){
            sb.append(character);
        }
        return sb.toString();
    }

    private static String getSpaces(int amount) {
        return getNChars(amount, ' ');
    }

    public static List<String> resultToHeaderAndRows(SqlResult result, boolean printType) {
        return resultToHeaderAndRows(result, printType, DEFAULT_EXTRA_SPACING);
    }

    public static List<String> resultToHeaderAndRows(SqlResult result) {
        return resultToHeaderAndRows(result, true, DEFAULT_EXTRA_SPACING);
    }

    public static List<String> resultToHeaderAndRows(SqlResult result, boolean printType, int extraSpacing) {
        List<String> arrayResult = new ArrayList<>();
        SqlRowMetadata rowMetadata = result.getRowMetadata();
        StringBuilder header = new StringBuilder();
        List<Integer> headerLengths = new ArrayList<>(Collections.nCopies(rowMetadata.getColumnCount(), 0));
        Iterator<SqlRow> resultIterator = result.iterator();
        LinkedList<SqlRow> sqlRows = new LinkedList<>();
        resultIterator.forEachRemaining(sqlRows::add);
        // Get max length of content
        for (SqlRow sqlRow : sqlRows) {
            for (int i = 0; i < rowMetadata.getColumnCount(); i++) {
                headerLengths.set(i, Math.max(headerLengths.get(i), sqlRow.getObject(i).toString().length()));
            }
        }
        for (int i = 0; i < rowMetadata.getColumnCount(); i++) {
            SqlColumnMetadata columnMetadata = rowMetadata.getColumn(i);
            String name = columnMetadata.getName();
            String typeName = columnMetadata.getType().getValueClass().getSimpleName();
            int nameLength = name.length();
            if (printType) {
                nameLength += (3 + typeName.length());
            }
            headerLengths.set(i, Math.max(headerLengths.get(i), nameLength) + extraSpacing);
            header.append("|");
            header.append(name);
            if (printType) {
                header.append(" (");
                header.append(typeName);
                header.append(")");
            }
            header.append(getSpaces(headerLengths.get(i) - nameLength));
            if (i >= rowMetadata.getColumnCount() - 1) {
                header.append("|");
            }
        }
        String headerString = header.toString();
        StringBuilder horizontalBar = new StringBuilder();
        for (int i = 0; i < rowMetadata.getColumnCount(); i++) {
            horizontalBar.append("+");
            horizontalBar.append(getNChars(headerLengths.get(i), '-'));
            if (i >= rowMetadata.getColumnCount() - 1) {
                horizontalBar.append("+");
            }
        }
        String horizontalBarString = horizontalBar.toString();
        arrayResult.add(horizontalBarString);
        arrayResult.add(headerString);
        arrayResult.add(horizontalBarString);
        for (SqlRow sqlRow : sqlRows) {
            StringBuilder row = new StringBuilder();
            for (int i = 0; i < rowMetadata.getColumnCount(); i++) {
                SqlColumnMetadata columnMetadata = rowMetadata.getColumn(i);
                String value = "N/A";
                switch (columnMetadata.getType()) {
                    case VARCHAR:
                        value = sqlRow.getObject(i);
                        break;
                    case TINYINT:
                    case BIGINT:
                    case SMALLINT:
                    case INTEGER:
                        Object integer = sqlRow.getObject(i); // Must be in 2 lines
                        value = String.format("%d", integer);
                        break;
                    case DOUBLE:
                    case REAL:
                    case DECIMAL:
                        Object decimal = sqlRow.getObject(i);
                        value = String.format("%f", decimal);
                        break;
                    case BOOLEAN:
                        Object bool = sqlRow.getObject(i);
                        value = String.format("%b", bool);
                        break;
                    case DATE:
                    case NULL:
                    case TIME:
                    case OBJECT:
                    case TIMESTAMP:
                    case TIMESTAMP_WITH_TIME_ZONE:
                        value = String.format("%s", sqlRow.getObject(i).toString());
                        break;
                }
                int valueLength = value.length();
                row.append("|");
                row.append(value);
                row.append(getSpaces(headerLengths.get(i) - valueLength));
                if (i >= rowMetadata.getColumnCount() - 1) {
                    row.append("|");
                }
            }
            arrayResult.add(row.toString());
        }
        arrayResult.add(horizontalBarString);
        return arrayResult;
    }
}
