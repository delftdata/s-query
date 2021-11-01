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

package com.hazelcast.jet.impl;

import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.impl.JobExecutionRecord.SnapshotStats;
import com.hazelcast.jet.impl.execution.SnapshotFlags;
import com.hazelcast.jet.impl.execution.init.ExecutionPlan;
import com.hazelcast.jet.impl.operation.SnapshotPhase2Operation;
import com.hazelcast.jet.impl.operation.SnapshotPhase1Operation;
import com.hazelcast.jet.impl.operation.SnapshotPhase1Operation.SnapshotPhase1Result;
import com.hazelcast.jet.impl.processor.IMapStateHelper;
import com.hazelcast.jet.impl.processor.SnapshotIMapKey;
import com.hazelcast.jet.impl.processor.TransformStatefulP;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.core.JobStatus.RUNNING;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.impl.JobRepository.EXPORTED_SNAPSHOTS_PREFIX;
import static com.hazelcast.jet.impl.JobRepository.exportedSnapshotMapName;
import static com.hazelcast.jet.impl.JobRepository.snapshotDataMapName;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Part of {@link MasterContext} that deals with snapshot creation.
 */
class MasterSnapshotContext {

    @SuppressWarnings("WeakerAccess") // accessed from subclass in jet-enterprise
    final MasterContext mc;
    private final ILogger logger;

    /**
     * It's true while a snapshot is in progress. It's used to prevent
     * concurrent snapshots.
     */
    private boolean snapshotInProgress;

    /**
     * A future (re)created when the job is started and completed when terminal
     * snapshot is completed (successfully or not).
     */
    @Nonnull
    private volatile CompletableFuture<Void> terminalSnapshotFuture = completedFuture(null);

    /**
     * The queue with snapshots to run. An item is added to it regularly (to do
     * a regular snapshot) or when a snapshot export is requested by the user.
     * <p>
     * The tuple contains:<ul>
     * <li>{@code snapshotMapName}: user-specified name of the snapshot or
     * null, if no name is specified
     * <li>{@code isTerminal}: if true, execution will be terminated after the
     * snapshot
     * <li>{@code future}: future, that will be completed when the snapshot
     * is validated.
     * </ul>
     * <p>
     * Queue is accessed only in synchronized code.
     */
    private final Queue<Tuple3<String, Boolean, CompletableFuture<Void>>> snapshotQueue = new LinkedList<>();

    // Fields for snapshot count down latch
    private ICountDownLatch ssCountDownLatch;
    private boolean distObjectInitialized;
    private final List<IMap<SnapshotIMapKey<Object>, Object>> snapshotIMaps = new ArrayList<>();
    private final List<IMap<SnapshotIMapKey<Object>, Object>> phaseSnapshotIMaps = new ArrayList<>();
    private IAtomicLong distSnapshotId;

    // Timer variables
    private long beforePhase1;
    private long beforePhase2;
    private long afterPhase2;

    // Benchmark times (in nanoseconds)
    private List<Long> imapStateSnapshotTimes;
    private List<Long> phase1SnapshotTimes;
    private List<Long> phase2SnapshotTimes;
    private boolean benchmarkListsInitialized;

    MasterSnapshotContext(MasterContext masterContext, ILogger logger) {
        mc = masterContext;
        this.logger = logger;
    }

    /**
     * Helper method initializing benchmark times. Will only initialize once.
     */
    private void initBenchmarkListsIfNotInitialized() {
        if (benchmarkListsInitialized) {
            return;
        }
        if (IMapStateHelper.isSnapshotStateEnabled(mc.getJetService().getConfig())) {
            String iMapTimesListName = IMapStateHelper.getBenchmarkIMapTimesListName(mc.jobName());
            imapStateSnapshotTimes = mc.getJetService().getJetInstance().getHazelcastInstance().getList(iMapTimesListName);
        }
        String phase1ListName = IMapStateHelper.getBenchmarkPhase1TimesListName(mc.jobName());
        String phase2ListName = IMapStateHelper.getBenchmarkPhase2TimesListName(mc.jobName());
        phase1SnapshotTimes = mc.getJetService().getJetInstance().getHazelcastInstance().getList(phase1ListName);
        phase2SnapshotTimes = mc.getJetService().getJetInstance().getHazelcastInstance().getList(phase2ListName);
        benchmarkListsInitialized = true;
    }

    /**
     * Helper method that initializes the countdown latch and snapshot IMap if it was not initialized already.
     */
    private void initDistObjectsIfNotInitialized() {
        if (distObjectInitialized) {
            return;
        }
        // Initialize distributed snapshot id
        String snapshotIdName = IMapStateHelper.getSnapshotIdName(mc.jobName());
        distSnapshotId = mc.getJetService().getJetInstance().getHazelcastInstance().getCPSubsystem()
                .getAtomicLong(snapshotIdName);

        // Early quit
        if (!IMapStateHelper.isSnapshotOrPhaseEnabled(mc.getJetService().getConfig())) {
            return;
        }

        Set<String> vertexNames = new HashSet<>();
        mc.executionPlanMap().forEach(((memberInfo, executionPlan) -> executionPlan.getVertices().forEach(vertexDef -> {
            Processor p = new ArrayList<>(vertexDef.processorSupplier().get(1)).get(0);
            if (p instanceof TransformStatefulP) {
                String vertexName = vertexDef.name();
                vertexNames.add(vertexName);
            }
        })));

        // For phase state only
        if (IMapStateHelper.isPhaseStateEnabled(mc.getJetService().getConfig())) {
            Stream<String> phaseSnapshotMapNames = vertexNames.stream().map(IMapStateHelper::getPhaseSnapshotMapName);
            phaseSnapshotMapNames = phaseSnapshotMapNames.peek(mapName ->
                    logger.info("Phase snapshot IMap name to evict from in master context: " + mapName));
            phaseSnapshotMapNames.forEach(mapName -> phaseSnapshotIMaps.add(
                    mc.getJetService().getJetInstance().getHazelcastInstance().getMap(mapName)));
        }

        // For snapshot state
        if (IMapStateHelper.isSnapshotStateEnabled(mc.getJetService().getConfig())) {
            String clusterCdlName = IMapStateHelper.clusterCountdownLatchHelper(mc.jobName());
            logger.info("Initializing cluster countdown latch: " + clusterCdlName);
            ssCountDownLatch = mc.getJetService().getJetInstance().getHazelcastInstance().getCPSubsystem()
                    .getCountDownLatch(clusterCdlName);

            // Get snapshot IMap names, then get the IMap objects and add them to the IMap list
            Stream<String> snapshotMapNames = vertexNames.stream().map(IMapStateHelper::getSnapshotMapName);
            snapshotMapNames = snapshotMapNames.peek(mapName ->
                    logger.info("Snapshot IMap name to evict from in master context: " + mapName));
            snapshotMapNames.forEach(mapName -> snapshotIMaps.add(
                    mc.getJetService().getJetInstance().getHazelcastInstance().getMap(mapName)));
        }
        distObjectInitialized = true;
    }

    @SuppressWarnings("SameParameterValue")
        // used by jet-enterprise
    void enqueueSnapshot(String snapshotMapName, boolean isTerminal, CompletableFuture<Void> future) {
        snapshotQueue.add(tuple3(snapshotMapName, isTerminal, future));
    }

    void startScheduledSnapshot(long executionId) {
        mc.lock();
        try {
            if (mc.jobStatus() != RUNNING) {
                logger.fine("Not beginning snapshot, " + mc.jobIdString() + " is not RUNNING, but " + mc.jobStatus());
                return;
            }
            if (mc.executionId() != executionId) {
                // Current execution is completed and probably a new execution has started, but we don't
                // cancel the scheduled snapshot from previous execution, so let's just ignore it.
                logger.fine("Not beginning snapshot since unexpected execution ID received for " + mc.jobIdString()
                        + ". Received execution ID: " + idToString(executionId));
                return;
            }
            enqueueSnapshot(null, false, null);
        } finally {
            mc.unlock();
        }
        tryBeginSnapshot();
    }

    void prepareDistObjects(long newSnapshotId) {
        // Countdown latch across members
        initDistObjectsIfNotInitialized();
        if (IMapStateHelper.isSnapshotStateEnabled(mc.getJetService().getConfig())) {
            logger.info(String.format("Total number of members for this job: %s, %d. Setting cluster latch. SSId: %d",
                    mc.jobName(), mc.executionPlanMap().size(), newSnapshotId));
            boolean succeeded = ssCountDownLatch.trySetCount(mc.executionPlanMap().size());
            if (!succeeded) {
                logger.severe(String.format(
                        "Countdown latch not fully counted down for job: %s, snapshot id: %d",
                        mc.jobName(),
                        newSnapshotId));
            }
            // Set snapshot id synchronously
            long snapshotIdStart = System.nanoTime();
            distSnapshotId.set(newSnapshotId);
            long snapshotIdEnd = System.nanoTime();
            logger.info(String.format("Set snapshot id atomic long to %d took: %d",
                    newSnapshotId, (snapshotIdEnd - snapshotIdStart)));
            // Wait for cluster cdl to complete async
            CompletableFuture.runAsync(() -> {
                try {
                    long clusterCdlStart = System.nanoTime();
                    boolean result =
                            ssCountDownLatch.await(mc.jobConfig().getSnapshotIntervalMillis(), TimeUnit.MILLISECONDS);
                    if (!result) {
                        logger.severe("Cluster snapshot countdown latch was not fully counted down in time! " +
                                "Still continuing with other snapshot process for job: " + mc.jobName());
                        return;
                    }
                    long clusterCdlEnd = System.nanoTime();
                    logger.info("Cluster level cdl took: " + (clusterCdlEnd - clusterCdlStart));
                    imapStateSnapshotTimes.add(clusterCdlEnd - clusterCdlStart);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            });

            // Evict older snapshot entries in each snapshot IMap
            long executeStart = System.nanoTime();
            snapshotIMaps.parallelStream().forEach(
                    imap -> imap.removeAll(IMapStateHelper.filterOldSnapshots(newSnapshotId,
                            mc.getJetService().getConfig())));
            long executeEnd = System.nanoTime();
            logger.info("Execute evictor on snapshot maps took: " + (executeEnd - executeStart));
        }

        if (IMapStateHelper.isPhaseStateEnabled(mc.getJetService().getConfig()) &&
                IMapStateHelper.isRemoveInMasterEnabled(mc.getJetService().getConfig())) {
            // Evict older snapshot entries in each phase snapshot IMap
            long executeStart = System.nanoTime();
            phaseSnapshotIMaps.parallelStream().forEach(
                    imap -> imap.removeAll(IMapStateHelper.filterOldSnapshots(newSnapshotId,
                            mc.getJetService().getConfig())));
            long executeEnd = System.nanoTime();
            logger.info("Execute evictor on phase maps took : " + (executeEnd - executeStart));
        }
    }

    void tryBeginSnapshot() {
        mc.coordinationService().submitToCoordinatorThread(() -> {
            boolean isTerminal;
            String snapshotMapName;
            CompletableFuture<Void> future;
            mc.lock();
            long localExecutionId;
            try {
                if (mc.jobStatus() != RUNNING) {
                    logger.fine("Not beginning snapshot, " + mc.jobIdString() + " is not RUNNING, but " + mc.jobStatus());
                    return;
                }
                if (snapshotInProgress) {
                    logger.fine("Not beginning snapshot since one is already in progress " + mc.jobIdString());
                    return;
                }
                if (terminalSnapshotFuture.isDone()) {
                    logger.fine("Not beginning snapshot since terminal snapshot is already completed " + mc.jobIdString());
                    return;
                }

                Tuple3<String, Boolean, CompletableFuture<Void>> requestedSnapshot = snapshotQueue.poll();
                if (requestedSnapshot == null) {
                    return;
                }
                snapshotInProgress = true;
                snapshotMapName = requestedSnapshot.f0();
                assert requestedSnapshot.f1() != null;
                isTerminal = requestedSnapshot.f1();
                future = requestedSnapshot.f2();
                mc.jobExecutionRecord().startNewSnapshot(snapshotMapName);
                localExecutionId = mc.executionId();
            } finally {
                mc.unlock();
            }

            mc.writeJobExecutionRecord(false);
            long newSnapshotId = mc.jobExecutionRecord().ongoingSnapshotId();
            boolean isExport = snapshotMapName != null;
            int snapshotFlags = SnapshotFlags.create(isTerminal, isExport);
            String finalMapName = isExport ? exportedSnapshotMapName(snapshotMapName)
                    : snapshotDataMapName(mc.jobId(), mc.jobExecutionRecord().ongoingDataMapIndex());
            mc.nodeEngine().getHazelcastInstance().getMap(finalMapName).clear();
            logFine(logger, "Starting snapshot %d for %s, flags: %s, writing to: %s",
                    newSnapshotId, mc.jobIdString(), SnapshotFlags.toString(snapshotFlags), snapshotMapName);

            // Init benchmark lists always
            initBenchmarkListsIfNotInitialized();

            prepareDistObjects(newSnapshotId);

            Function<ExecutionPlan, Operation> factory = plan ->
                    new SnapshotPhase1Operation(mc.jobId(), localExecutionId, newSnapshotId, finalMapName, snapshotFlags);

            beforePhase1 = System.nanoTime();
            // Need to take a copy of executionId: we don't cancel the scheduled task when the execution
            // finalizes. If a new execution is started in the meantime, we'll use the execution ID to detect it.
            mc.invokeOnParticipants(
                    factory,
                    responses -> onSnapshotPhase1Complete(responses, localExecutionId, newSnapshotId, finalMapName,
                            snapshotFlags, future),
                    null, true);
        });
    }

    /**
     * @param responses       collected responses from the members
     * @param snapshotMapName the IMap name to which the snapshot is written
     * @param snapshotFlags   flags of the snapshot
     * @param future          a future to be completed when the phase-2 is fully completed
     */
    private void onSnapshotPhase1Complete(
            Collection<Map.Entry<MemberInfo, Object>> responses,
            long executionId,
            long snapshotId,
            String snapshotMapName,
            int snapshotFlags,
            @Nullable CompletableFuture<Void> future
    ) {
        mc.coordinationService().submitToCoordinatorThread(() -> {
            SnapshotPhase1Result mergedResult = new SnapshotPhase1Result();
            for (Map.Entry<MemberInfo, Object> entry : responses) {
                // the response is either SnapshotOperationResult or an exception, see #invokeOnParticipants() method
                Object response = entry.getValue();
                if (response instanceof Throwable) {
                    response = new SnapshotPhase1Result(0, 0, 0, (Throwable) response);
                }
                mergedResult.merge((SnapshotPhase1Result) response);
            }
            boolean isSuccess;
            SnapshotStats stats;

            mc.lock();
            try {
                // Note: this method can be called after finalizeJob() is called or even after new execution started.
                // Check the execution ID to check if a new execution didn't start yet.
                if (executionId != mc.executionId()) {
                    LoggingUtil.logFine(logger, "%s: ignoring responses for snapshot %s phase 1: " +
                                    "the responses are from a different execution: %s. Responses: %s",
                            mc.jobIdString(), snapshotId, idToString(executionId), responses);
                    return;
                }

                IMap<Object, Object> snapshotMap = mc.nodeEngine().getHazelcastInstance().getMap(snapshotMapName);
                try {
                    SnapshotValidationRecord validationRecord = new SnapshotValidationRecord(snapshotId,
                            mergedResult.getNumChunks(), mergedResult.getNumBytes(),
                            mc.jobExecutionRecord().ongoingSnapshotStartTime(), mc.jobId(), mc.jobName(),
                            mc.jobRecord().getDagJson());

                    // The decision moment for exported snapshots: after this the snapshot is valid to be restored
                    // from, however it will be not listed by JetInstance.getJobStateSnapshots unless the validation
                    // record is inserted into the cache below
                    Object oldValue = snapshotMap.put(SnapshotValidationRecord.KEY, validationRecord);

                    if (snapshotMapName.startsWith(EXPORTED_SNAPSHOTS_PREFIX)) {
                        String snapshotName = snapshotMapName.substring(EXPORTED_SNAPSHOTS_PREFIX.length());
                        mc.jobRepository().cacheValidationRecord(snapshotName, validationRecord);
                    }
                    if (oldValue != null) {
                        logger.severe("SnapshotValidationRecord overwritten after writing to '" + snapshotMapName
                                + "' for " + mc.jobIdString() + ": snapshot data might be corrupted");
                    }
                } catch (Exception e) {
                    mergedResult.merge(new SnapshotPhase1Result(0, 0, 0, e));
                }

                isSuccess = mergedResult.getError() == null;
                stats = mc.jobExecutionRecord().ongoingSnapshotDone(
                        mergedResult.getNumBytes(), mergedResult.getNumKeys(), mergedResult.getNumChunks(),
                        mergedResult.getError());

                // the decision moment for regular snapshots: after this the snapshot is ready to be restored from
                mc.writeJobExecutionRecord(false);

                if (logger.isFineEnabled()) {
                    logger.fine(String.format("Snapshot %d phase 1 for %s completed with status %s in %dms, " +
                                    "%,d bytes, %,d keys in %,d chunks, stored in '%s', proceeding to phase 2",
                            snapshotId, mc.jobIdString(), isSuccess ? "SUCCESS" : "FAILURE",
                            stats.duration(), stats.numBytes(), stats.numKeys(), stats.numChunks(), snapshotMapName));
                }
                if (!isSuccess) {
                    logger.warning(mc.jobIdString() + " snapshot " + snapshotId + " phase 1 failed on some member(s), " +
                            "one of the failures: " + mergedResult.getError());
                    try {
                        snapshotMap.clear();
                    } catch (Exception e) {
                        logger.warning(mc.jobIdString() + ": failed to clear snapshot map '" + snapshotMapName
                                + "' after a failure", e);
                    }
                }
                if (!SnapshotFlags.isExport(snapshotFlags)) {
                    mc.jobRepository().clearSnapshotData(mc.jobId(), mc.jobExecutionRecord().ongoingDataMapIndex());
                }
            } finally {
                mc.unlock();
            }

            // start the phase 2
            Function<ExecutionPlan, Operation> factory = plan -> new SnapshotPhase2Operation(
                    mc.jobId(), executionId, snapshotId, isSuccess && !SnapshotFlags.isExportOnly(snapshotFlags));

            beforePhase2 = System.nanoTime();
            mc.invokeOnParticipants(factory,
                    responses2 -> onSnapshotPhase2Complete(mergedResult.getError(), responses2, executionId, snapshotId,
                            snapshotFlags, future, stats.startTime()),
                    null, true);
        });
    }

    /**
     * @param phase1Error   error from the phase-1. Null if phase-1 was successful.
     * @param responses     collected responses from the members
     * @param snapshotFlags flags of the snapshot
     * @param future        future to be completed when the phase-2 is fully completed
     * @param startTime     phase-1 start time
     */
    private void onSnapshotPhase2Complete(
            String phase1Error,
            Collection<Entry<MemberInfo, Object>> responses,
            long executionId,
            long snapshotId,
            int snapshotFlags,
            @Nullable CompletableFuture<Void> future,
            long startTime
    ) {
        mc.coordinationService().submitToCoordinatorThread(() -> {
            if (executionId != mc.executionId()) {
                LoggingUtil.logFine(logger, "%s: ignoring responses for snapshot %s phase 2: " +
                                "the responses are from a different execution: %s. Responses: %s",
                        mc.jobIdString(), snapshotId, idToString(executionId), responses);
                return;
            }

            for (Entry<MemberInfo, Object> response : responses) {
                if (response.getValue() instanceof Throwable) {
                    logger.warning(SnapshotPhase2Operation.class.getSimpleName() + " for snapshot " + snapshotId + " in "
                            + mc.jobIdString() + " failed on member: " + response, (Throwable) response.getValue());
                }
            }

            if (future != null) {
                if (phase1Error == null) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(new JetException(phase1Error));
                }
            }

            mc.lock();
            try {
                // double-check the execution ID after locking
                if (executionId != mc.executionId()) {
                    logger.fine("Not completing terminalSnapshotFuture on " + mc.jobIdString() + ", new execution " +
                            "already started, snapshot was for executionId=" + idToString(executionId));
                    return;
                }
                assert snapshotInProgress : "snapshot not in progress";
                snapshotInProgress = false;
                if (SnapshotFlags.isTerminal(snapshotFlags)) {
                    // after a terminal snapshot, no more snapshots are scheduled in this execution
                    boolean completedNow = terminalSnapshotFuture.complete(null);
                    assert completedNow : "terminalSnapshotFuture was already completed";
                    if (phase1Error != null) {
                        // If the terminal snapshot failed, the executions might not terminate on some members
                        // normally and we don't care if they do - the snapshot is done and we have to bring the
                        // execution down. Let's execute the CompleteExecutionOperation to terminate them.
                        mc.jobContext().cancelExecutionInvocations(mc.jobId(), mc.executionId(), null);
                    }
                } else if (!SnapshotFlags.isExport(snapshotFlags)) {
                    // if this snapshot was an automatic snapshot, schedule the next one
                    mc.coordinationService().scheduleSnapshot(mc, executionId);
                }
            } finally {
                mc.unlock();
            }
            // Set snapshot id synchronously
            afterPhase2 = System.nanoTime();
            long snapshotIdEnd = 0;
            if (IMapStateHelper.isSnapshotOrPhaseEnabled(mc.getJetService().getConfig())) {
                distSnapshotId.set(snapshotId);
                snapshotIdEnd = System.nanoTime();
            }


            if (logger.isFineEnabled()) {
                logger.fine("Snapshot " + snapshotId + " for " + mc.jobIdString() + " completed in "
                        + (System.currentTimeMillis() - startTime) + "ms, status="
                        + (phase1Error == null ? "success" : "failure: " + phase1Error));
            }
            if (IMapStateHelper.isSnapshotOrPhaseEnabled(mc.getJetService().getConfig())) {
                logger.info(String.format("Set snapshot id atomic long to %d took: %d",
                        snapshotId, (snapshotIdEnd - afterPhase2)));
            }
            // Add times to benchmark lists
            phase1SnapshotTimes.add(beforePhase2 - beforePhase1);
            phase2SnapshotTimes.add(afterPhase2 - beforePhase2);

            tryBeginSnapshot();
        });
    }

    CompletableFuture<Void> terminalSnapshotFuture() {
        return terminalSnapshotFuture;
    }

    void onExecutionStarted() {
        snapshotInProgress = false;
        assert snapshotQueue.isEmpty() : "snapshotQueue not empty";
        terminalSnapshotFuture = new CompletableFuture<>();
    }

    void onExecutionTerminated() {
        for (Tuple3<String, Boolean, CompletableFuture<Void>> snapshotTuple : snapshotQueue) {
            if (snapshotTuple.f2() != null) {
                snapshotTuple.f2().completeExceptionally(
                        new JetException("Execution completed before snapshot executed"));
            }
        }
        snapshotQueue.clear();
    }

    public ILogger logger() {
        return logger;
    }
}
