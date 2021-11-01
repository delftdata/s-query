package org.example;

import com.hazelcast.jet.impl.processor.SnapshotIMapKey;
import com.hazelcast.query.Predicate;

import java.util.Map;

/**
 * Helper class for getting latest snapshot items with certain key range
 */
public class SnapshotRangePredicate implements Predicate<SnapshotIMapKey<Long>, Object> {
    private final long curSnapshotId;
    private final long range;

    public SnapshotRangePredicate(long curSnapshotId, long range) {
        this.curSnapshotId = curSnapshotId;
        this.range = range;
    }

    @Override
    public boolean apply(Map.Entry<SnapshotIMapKey<Long>, Object> mapEntry) {
        return mapEntry.getKey().getPartitionKey() < range && mapEntry.getKey().getSnapshotId() == curSnapshotId;
    }
}
