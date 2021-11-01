package org.example;

import com.hazelcast.jet.impl.processor.SnapshotIMapKey;
import com.hazelcast.query.Predicate;

import java.util.Map;

/**
 * Helper class for getting latest snapshot items
 */
public class SnapshotPredicate implements Predicate<SnapshotIMapKey<Long>, Object> {
    private final long curSnapshotId;

    public SnapshotPredicate(long curSnapshotId) {
        this.curSnapshotId = curSnapshotId;
    }

    @Override
    public boolean apply(Map.Entry<SnapshotIMapKey<Long>, Object> mapEntry) {
        return mapEntry.getKey().getSnapshotId() == curSnapshotId;
    }
}