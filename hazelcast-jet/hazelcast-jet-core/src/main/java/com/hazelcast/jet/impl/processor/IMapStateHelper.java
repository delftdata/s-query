/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

public final class IMapStateHelper {
    // Properties
    static final HazelcastProperty SNAPSHOT_STATE
            = new HazelcastProperty("state.snapshot", false);
    private static final HazelcastProperty PHASE_STATE
            = new HazelcastProperty("state.phase", false);
    private static final HazelcastProperty LIVE_STATE
            = new HazelcastProperty("state.live", false);
    private static final HazelcastProperty LIVE_STATE_ASYNC
            = new HazelcastProperty("state.live.async", false);
    private static final HazelcastProperty PHASE_BATCH
            = new HazelcastProperty("state.phase.batch", false);
    private static final HazelcastProperty PHASE_BATCH_WATERMARK
            = new HazelcastProperty("state.phase.batch.watermark", false);
    private static final HazelcastProperty PHASE_BATCH_CONCURRENT
            = new HazelcastProperty("state.phase.batch.concurrent", false);
    private static final HazelcastProperty WAIT_FOR_FUTURES
            = new HazelcastProperty("wait.for.futures", false);
    private static final HazelcastProperty BLOB_STATE
            = new HazelcastProperty("state.blob", true);
    private static final HazelcastProperty MEMORY_FORMAT_OBJECT
            = new HazelcastProperty("memory.format.object", false);
    private static final HazelcastProperty FAST_SNAPSHOT
            = new HazelcastProperty("state.phase.setentries", false);
    private static final HazelcastProperty STATISTICS
            = new HazelcastProperty("state.phase.statistics", true);
    private static final HazelcastProperty BACKUP
            = new HazelcastProperty("state.phase.backup", true);
    private static final HazelcastProperty REMOVE_IN_MASTER =
            new HazelcastProperty("state.remove.master", false);
    private static final HazelcastProperty SNAPSHOTS_TO_KEEP =
            new HazelcastProperty("state.snapshotnum", 2);
    private static final HazelcastProperty SNAPSHOT_DEBUG =
            new HazelcastProperty("state.debug.snapshot", false);
    private static final HazelcastProperty REMOVE_DEBUG =
            new HazelcastProperty("state.debug.remove", false);
    private static final HazelcastProperty INCREMENTAL_SNASPHOT =
            new HazelcastProperty("state.phase.incremental", false);
    private static final HazelcastProperty INCREMENTAL_REMOVEALL =
            new HazelcastProperty("state.phase.incremental.removeall", false);


    // Booleans which control if IMap state is used or not
    private static boolean snapshotStateEnabled; // Toggle for snapshot state
    private static boolean phaseStateEnabled; // Toggle for phase (2) snapshot state
    private static boolean liveStateEnabled; // Toggle for live state
    private static boolean liveStateAsync; // Toggle for live state async
    private static boolean phaseStateBatchEnabled; // Toggle for batched phase state
    private static boolean phaseStateBatchWatermarkEnabled; // Toggle for batched phase state on watermark
    private static boolean phaseStateBatchConcurrentEnabled; // Toggle for batched phase state in concurrent hashmap
    private static boolean waitForFuturesEnabled; // Toggle for wait for futures
    private static boolean blobStateEnabled; // Toggle for serialized blob state
    private static boolean memoryFormatObject; // Toggle for IMap memory object format, false = BINARY
    private static boolean fastSnapshot; // Toggle for fast snapshot put method using setEntries()
    private static boolean staticsEnabled; // Toggle for statistic enabled on snapshot IMap
    private static boolean isEnableBackup; // Toggle for enabling backup copies for snapshot IMap
    // Toggle for removing old snapshot Ids in mastersnapshotcontext (true) or asyncsnapshotwriter (false)
    private static boolean isRemoveInMaster;
    private static boolean isDebugSnap; // Toggle for enabling logging of IMap times
    private static boolean isDebugRem; // Toggle for enabling logging of remove from IMap times
    private static int snapshotsToKeep; // Amount of snapshots to keep
    private static boolean incrementalSnapshot; // Toggle for incremental snapshots
    private static boolean incrementalRemoveAll; // Toggle for removing all old snapshots at once

    // Used to keep track if imap state boolean is already cached
    private static boolean snapshotStateEnabledCached;
    private static boolean phaseStateEnabledCached;
    private static boolean liveStateEnabledCached;
    private static boolean liveStateAsyncCached;
    private static boolean phaseStateBatchEnabledCached;
    private static boolean phaseStateBatchWatermarkEnabledCached;
    private static boolean phaseStateBatchConcurrentEnabledCached;
    private static boolean waitForFuturesEnabledCached;
    private static boolean blobStateEnabledCached;
    private static boolean memoryFormatObjectCached;
    private static boolean fastSnapshotCached;
    private static boolean staticsEnabledCached;
    private static boolean isEnableBackupCached;
    private static boolean isRemoveInMasterCached;
    private static boolean snapshotsToKeepCached;
    private static boolean isDebugSnapCached;
    private static boolean isDebugRemCached;
    private static boolean incrementalSnapshotCached;
    private static boolean incrementalRemoveAllCached;

    // Private constructor to prevent instantiation
    private IMapStateHelper() {

    }

    private static boolean getBool(JetConfig config, HazelcastProperty property) {
        if (config == null) {
            return Boolean.parseBoolean(property.getDefaultValue());
        }
        return new HazelcastProperties(config.getProperties()).getBoolean(property);
    }

    private static int getInt(JetConfig config, HazelcastProperty property) {
        if (config == null) {
            return Integer.parseInt(property.getDefaultValue());
        }
        return new HazelcastProperties(config.getProperties()).getInteger(property);
    }

    /**
     * Helper method that gets whether the snapshot IMap state is enabled.
     *
     * @param config The JetConfig where the "state.snapshot" property should be "true" of "false"
     * @return True if the config says true, false if it says false.
     */
    public static boolean isSnapshotStateEnabled(JetConfig config) {
        if (!snapshotStateEnabledCached) {
            snapshotStateEnabled = getBool(config, SNAPSHOT_STATE);
            snapshotStateEnabledCached = true;
        }
        return snapshotStateEnabled;
    }

    public static boolean isPhaseStateEnabled(JetConfig config) {
        if (!phaseStateEnabledCached) {
            phaseStateEnabled = getBool(config, PHASE_STATE);
            phaseStateEnabledCached = true;
        }
        return phaseStateEnabled;
    }

    public static boolean isLiveStateEnabled(JetConfig config) {
        if (!liveStateEnabledCached) {
            liveStateEnabled = getBool(config, LIVE_STATE);
            liveStateEnabledCached = true;
        }
        return liveStateEnabled;
    }

    public static boolean isLiveStateAsync(JetConfig config) {
        if (!liveStateAsyncCached) {
            liveStateAsync = getBool(config, LIVE_STATE_ASYNC);
            liveStateAsyncCached = true;
        }
        return liveStateAsync;
    }

    public static boolean isBatchPhaseStateEnabled(JetConfig config) {
        if (!phaseStateBatchEnabledCached) {
            phaseStateBatchEnabled = getBool(config, PHASE_BATCH);
            phaseStateBatchEnabledCached = true;
        }
        return phaseStateBatchEnabled;
    }

    public static boolean isBatchPhaseStateWatermarkEnabled(JetConfig config) {
        if (!phaseStateBatchWatermarkEnabledCached) {
            phaseStateBatchWatermarkEnabled = getBool(config, PHASE_BATCH_WATERMARK);
            phaseStateBatchWatermarkEnabledCached = true;
        }
        return phaseStateBatchWatermarkEnabled;
    }

    public static boolean isBatchPhaseConcurrentEnabled(JetConfig config) {
        if (!phaseStateBatchConcurrentEnabledCached) {
            phaseStateBatchConcurrentEnabled = getBool(config, PHASE_BATCH_CONCURRENT);
            phaseStateBatchConcurrentEnabledCached = true;
        }
        return phaseStateBatchConcurrentEnabled;
    }

    public static boolean isWaitForFuturesEnabled(JetConfig config) {
        if (!waitForFuturesEnabledCached) {
            waitForFuturesEnabled = getBool(config, WAIT_FOR_FUTURES);
            waitForFuturesEnabledCached = true;
        }
        return waitForFuturesEnabled;
    }

    public static boolean isBlobStateEnabled(JetConfig config) {
        if (!blobStateEnabledCached) {
            blobStateEnabled = getBool(config, BLOB_STATE);
            blobStateEnabledCached = true;
        }
        return blobStateEnabled;
    }

    public static boolean isMemoryFormatObject(JetConfig config) {
        if (!memoryFormatObjectCached) {
            memoryFormatObject = getBool(config, MEMORY_FORMAT_OBJECT);
            memoryFormatObjectCached = true;
        }
        return memoryFormatObject;
    }

    public static boolean isFastSnapshotEnabled(JetConfig config) {
        if (!fastSnapshotCached) {
            fastSnapshot = getBool(config, FAST_SNAPSHOT);
            fastSnapshotCached = true;
        }
        return fastSnapshot;
    }

    public static boolean isStatisticsEnabled(JetConfig config) {
        if (!staticsEnabledCached) {
            staticsEnabled = getBool(config, STATISTICS);
            staticsEnabledCached = true;
        }
        return staticsEnabled;
    }

    public static boolean enableBackup(JetConfig config) {
        if (!isEnableBackupCached) {
            isEnableBackup = getBool(config, BACKUP);
            isEnableBackupCached = true;
        }
        return isEnableBackup;
    }

    public static boolean isRemoveInMasterEnabled(JetConfig config) {
        if (!isRemoveInMasterCached) {
            isRemoveInMaster = getBool(config, REMOVE_IN_MASTER);
            isRemoveInMasterCached = true;
        }
        return isRemoveInMaster;
    }

    public static boolean isDebugSnapshot(JetConfig config) {
        if (!isDebugSnapCached) {
            isDebugSnap = getBool(config, SNAPSHOT_DEBUG);
            isDebugSnapCached = true;
        }
        return isDebugSnap;
    }

    public static boolean isDebugRemove(JetConfig config) {
        if (!isDebugRemCached) {
            isDebugRem = getBool(config, REMOVE_DEBUG);
            isDebugRemCached = true;
        }
        return isDebugRem;
    }

    public static int getSnapshotsToKeep(JetConfig config) {
        if (!snapshotsToKeepCached) {
            snapshotsToKeep = getInt(config, SNAPSHOTS_TO_KEEP);
            snapshotsToKeepCached = true;
        }
        return snapshotsToKeep;
    }

    public static boolean isIncrementalSnapshot(JetConfig config) {
        if (!incrementalSnapshotCached) {
            incrementalSnapshot = getBool(config, INCREMENTAL_SNASPHOT);
            incrementalSnapshotCached = true;
        }
        return incrementalSnapshot;
    }

    public static boolean isIncrementalRemoveAll(JetConfig config) {
        if (!incrementalRemoveAllCached) {
            incrementalRemoveAll = getBool(config, INCREMENTAL_REMOVEALL);
            incrementalRemoveAllCached = true;
        }
        return incrementalRemoveAll;
    }

    public static boolean isSnapshotOrPhaseEnabled(JetConfig config) {
        return isSnapshotStateEnabled(config) || isPhaseStateEnabled(config);
    }

    public static String getBenchmarkIMapTimesListName(String jobName) {
        return String.format("benchmark-imap-%s", jobName);
    }

    public static String getBenchmarkPhase1TimesListName(String jobName) {
        return String.format("benchmark-phase1-%s", jobName);
    }

    public static String getBenchmarkPhase2TimesListName(String jobName) {
        return String.format("benchmark-phase2-%s", jobName);
    }

    /**
     * Helper method for live state IMap name.
     *
     * @param vertexName The name of the transform
     * @return The live state IMap name for the given transform
     */
    public static String getLiveStateImapName(String vertexName) {
        return vertexName;
    }

    /**
     * Helper method for snapshot state IMap name.
     *
     * @param vertexName The name of the transform
     * @return The snapshot state IMap name for the given transform
     */
    public static String getSnapshotMapName(String vertexName) {
        return MessageFormat.format("old_snapshot_{0}", vertexName);
    }

    /**
     * Helper method for phase (2) snapshot state IMap name.
     *
     * @param vertexName The name of the transform
     * @return The phase snapshot state IMap name for the given transform
     */
    public static String getPhaseSnapshotMapName(String vertexName) {
        return MessageFormat.format("snapshot_{0}", vertexName);
    }

    public static String getSnapshotIdName(String jobName) {
        return MessageFormat.format("ssid-{0}", jobName);
    }

    public static String clusterCountdownLatchHelper(String jobName) {
        return "cdl-" + jobName;
    }

    public static String memberCountdownLatchHelper(UUID memberName, String jobName) {
        return String.format("cdl-%s-%s", memberName.toString(), jobName);
    }

    /**
     * Predicate helper that selects old snapshot items.
     * @param curSnapshotId The current snapshot ID
     * @return Predicate that returns true for items with
     * snapshot ID older than curSnapshotId - amount to keep
     * or snapshot ID newer then curSnapshotId, false otherwise
     */
    public static Predicate<SnapshotIMapKey<Object>, Object> filterOldSnapshots(long curSnapshotId, JetConfig config) {
        if (!snapshotsToKeepCached) {
            snapshotsToKeep = getSnapshotsToKeep(config);
        }
        return new FilterOldSnapshots(curSnapshotId, snapshotsToKeep);
    }

    /**
     * Predicate helper that selects old incremental snapshot items.
     * @param toRemove Set of SnapshotIMap keys to remove
     * @return Predicate that returns true for items with
     * key that are int he toRemove set.
     */
    @SuppressWarnings("IllegalType")
    public static Predicate<SnapshotIMapKey<Object>, Object> filterOldIncrementalSnapshots(
            HashSet<SnapshotIMapKey<Object>> toRemove, JetConfig config) {
        if (!snapshotsToKeepCached) {
            snapshotsToKeep = getSnapshotsToKeep(config);
        }
        return new FilterOldIncrementalSnapshots(toRemove);
    }

    /**
     * Helper class for filtering out old snapshot items.
     */
    public static class FilterOldSnapshots implements Predicate<SnapshotIMapKey<Object>, Object> {
        private final long curSnapshotId;
        private final int amountToKeep;

        public FilterOldSnapshots(long curSnapshotId, int amountToKeep) {
            this.curSnapshotId = curSnapshotId;
            this.amountToKeep = amountToKeep;
        }

        @Override
        public boolean apply(Map.Entry<SnapshotIMapKey<Object>, Object> mapEntry) {
            long ssid = mapEntry.getKey().getSnapshotId();
            return ssid <= curSnapshotId - amountToKeep || ssid > curSnapshotId;
        }
    }

    /**
     * Helper class for filtering out old incremental snapshot items.
     */
    public static class FilterOldIncrementalSnapshots implements Predicate<SnapshotIMapKey<Object>, Object> {
        private final HashSet<SnapshotIMapKey<Object>> toRemove; // Should be serializable

        public FilterOldIncrementalSnapshots(HashSet<SnapshotIMapKey<Object>> toRemove) {
            this.toRemove = toRemove;
        }

        @Override
        public boolean apply(Map.Entry<SnapshotIMapKey<Object>, Object> mapEntry) {
            return toRemove.contains(mapEntry.getKey());
        }
    }
}
