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

package com.hazelcast.jet.impl.util;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.serialization.impl.SerializationConstants;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.SnapshotContext;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.jet.impl.processor.IMapStateHelper;
import com.hazelcast.jet.impl.processor.SnapshotIMapKey;
import com.hazelcast.jet.impl.serialization.SerializerHookConstants;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.PartitionAware;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

public class AsyncSnapshotWriterImpl implements AsyncSnapshotWriter {

    public static final int DEFAULT_CHUNK_SIZE = 128 * 1024;
    private static final int MAP_SIZE = 1000; // For initial MapEntries array size
    private static final long REMOVE_TIMEOUT = 10L; // Timeout for removing old snapshot IDs from IMap (in seconds)

    final int usableChunkCapacity; // this includes the serialization header for byte[], but not the terminator
    final byte[] serializedByteArrayHeader = new byte[3 * Bits.INT_SIZE_IN_BYTES];
    final byte[] valueTerminator;
    final AtomicInteger numConcurrentAsyncOps;

    private final IPartitionService partitionService;

    private final CustomByteArrayOutputStream[] buffers;
    private final int[] partitionKeys;
    private final JetService jetService;
    private final ILogger logger;
    private final NodeEngine nodeEngine;
    private final boolean useBigEndian;
    private final SnapshotContext snapshotContext;
    private final String vertexName;
    private final int memberCount;
    private final AtomicReference<Throwable> firstError = new AtomicReference<>();
    private final AtomicInteger numActiveFlushes = new AtomicInteger();
    private final SerializationService serializationService;
    // Temporary state map
    private final Map<SnapshotIMapKey<Object>, Data> tempStateMap = new HashMap<>();
    private final Map<SnapshotIMapKey<Object>, Data> tempStateMapConc = new ConcurrentHashMap<>();
    // Known Snapshot ID map
    private final Map<Object, ArrayDeque<Long>> prevSnapshotIdMap = new HashMap<>();
    private final ArrayList<SnapshotIMapKey<Object>> snapshotsToRemoveList = new ArrayList<>();
    private final HashSet<SnapshotIMapKey<Object>> snapshotsToRemove = new HashSet<>();
    // Varialbe to keep track if put to state map is done in offer()
    private final AtomicReference<Boolean> putAsyncStateDone = new AtomicReference<>(false);
    // Lock for above temp state map
    private final AtomicReference<Boolean> tempStateMapLock = new AtomicReference<>(false);
    private final BiConsumer<Object, Throwable> putResponseConsumer = this::consumePutResponse;
    private MapEntries[] tempEntries;
    private int partitionSequence;
    private IMap<SnapshotDataKey, Object> currentMap;
    private IMap<SnapshotIMapKey<Object>, Object> stateMap;
//    private IMap<Object, Object> stateMap;
    private long currentSnapshotId;
    // stats
    private long totalKeys;
    private long totalChunks;
    private long totalPayloadBytes;

    public AsyncSnapshotWriterImpl(NodeEngine nodeEngine,
                                   SnapshotContext snapshotContext,
                                   String vertexName,
                                   int memberIndex,
                                   int memberCount,
                                   SerializationService serializationService) {
        this(DEFAULT_CHUNK_SIZE, nodeEngine, snapshotContext, vertexName, memberIndex, memberCount, serializationService);
    }

    // for test
    AsyncSnapshotWriterImpl(int chunkSize,
                            NodeEngine nodeEngine,
                            SnapshotContext snapshotContext,
                            String vertexName,
                            int memberIndex,
                            int memberCount,
                            SerializationService serializationService) {
        if (Integer.bitCount(chunkSize) != 1) {
            throw new IllegalArgumentException("chunkSize must be a power of two, but is " + chunkSize);
        }
        this.nodeEngine = nodeEngine;
        this.partitionService = nodeEngine.getPartitionService();
        this.logger = nodeEngine.getLogger(getClass());
        this.snapshotContext = snapshotContext;
        this.vertexName = vertexName;
        this.memberCount = memberCount;
        this.serializationService = serializationService;
        currentSnapshotId = snapshotContext.currentSnapshotId();

        useBigEndian = isBigEndian();

        buffers = createAndInitBuffers(chunkSize, partitionService.getPartitionCount(), serializedByteArrayHeader);
        this.jetService = nodeEngine.getService(JetService.SERVICE_NAME);
        this.partitionKeys = jetService.getSharedPartitionKeys();
        this.partitionSequence = memberIndex;

        this.numConcurrentAsyncOps = jetService.numConcurrentAsyncOps();

        byte[] valueTerminatorWithHeader = serializationService.toData(SnapshotDataValueTerminator.INSTANCE).toByteArray();
        valueTerminator = Arrays.copyOfRange(valueTerminatorWithHeader, HeapData.TYPE_OFFSET,
                valueTerminatorWithHeader.length);
        usableChunkCapacity = chunkSize - valueTerminator.length - serializedByteArrayHeader.length;
        checkChunkCapacity(chunkSize);
    }

    private static CustomByteArrayOutputStream[] createAndInitBuffers(
            int chunkSize,
            int partitionCount,
            byte[] serializedByteArrayHeader
    ) {
        CustomByteArrayOutputStream[] buffers = new CustomByteArrayOutputStream[partitionCount];
        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = new CustomByteArrayOutputStream(chunkSize);
            buffers[i].write(serializedByteArrayHeader, 0, serializedByteArrayHeader.length);
        }
        return buffers;
    }

    private boolean isBigEndian() {
        boolean bigEndian = !nodeEngine.getHazelcastInstance().getConfig().getSerializationConfig().isUseNativeByteOrder()
                || ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;
        Bits.writeInt(serializedByteArrayHeader, Bits.INT_SIZE_IN_BYTES, SerializationConstants.CONSTANT_TYPE_BYTE_ARRAY,
                bigEndian);
        return bigEndian;
    }

    private void checkChunkCapacity(int chunkSize) {
        if (usableChunkCapacity <= 0) {
            throw new IllegalArgumentException("too small chunk size: " + chunkSize);
        }
    }

    private void consumePutResponse(Object response, Throwable throwable) {
        try {
            assert response == null : "put operation overwrote a previous value: " + response;
        } catch (AssertionError e) {
            throwable = e;
        }
        if (throwable != null) {
            logger.severe("Error writing to snapshot map", throwable);
            firstError.compareAndSet(null, throwable);
        }
        numActiveFlushes.decrementAndGet();
        numConcurrentAsyncOps.decrementAndGet();
    }

    @Override
    @CheckReturnValue
    public boolean offer(Entry<? extends Data, ? extends Data> entry) {
        // Only execute putAsyncToStateMap once
        if (IMapStateHelper.isPhaseStateEnabled(jetService.getConfig())
                && !Boolean.TRUE.equals(putAsyncStateDone.get())) {
            boolean putAsyncStateResult = putAsyncToStateMap(entry);
            if (putAsyncStateResult) {
                putAsyncStateDone.set(true);
            } else {
                return false;
            }
        }

        if (IMapStateHelper.isBlobStateEnabled(jetService.getConfig())) {
            int partitionId = partitionService.getPartitionId(entry.getKey());
            int length = entry.getKey().totalSize() + entry.getValue().totalSize() - 2 * HeapData.TYPE_OFFSET;

            // if the entry is larger than usableChunkSize, send it in its own chunk. We avoid adding it to the
            // ByteArrayOutputStream since it would expand it beyond its maximum capacity.
            if (length > usableChunkCapacity) {
                boolean putAsyncResult = putAsyncToMap(partitionId, () -> {
                    byte[] data = new byte[serializedByteArrayHeader.length + length + valueTerminator.length];
                    totalKeys++;
                    int offset = 0;
                    System.arraycopy(serializedByteArrayHeader, 0, data, offset, serializedByteArrayHeader.length);
                    offset += serializedByteArrayHeader.length - Bits.INT_SIZE_IN_BYTES;

                    Bits.writeInt(data, offset, length + valueTerminator.length, useBigEndian);
                    offset += Bits.INT_SIZE_IN_BYTES;

                    copyWithoutHeader(entry.getKey(), data, offset);
                    offset += entry.getKey().totalSize() - HeapData.TYPE_OFFSET;

                    copyWithoutHeader(entry.getValue(), data, offset);
                    offset += entry.getValue().totalSize() - HeapData.TYPE_OFFSET;

                    System.arraycopy(valueTerminator, 0, data, offset, valueTerminator.length);

                    return new HeapData(data);
                });
                if (putAsyncResult) {
                    putAsyncStateDone.set(false);
                }
                return putAsyncResult;
            }

            // if the buffer after adding this entry and terminator would exceed the capacity limit, flush it first
            CustomByteArrayOutputStream buffer = buffers[partitionId];
            if (buffer.size() + length + valueTerminator.length > buffer.capacityLimit && !flushPartition(partitionId)) {
                return false;
            }

            // append to buffer
            writeWithoutHeader(entry.getKey(), buffer);
            writeWithoutHeader(entry.getValue(), buffer);
            totalKeys++;
        }
        putAsyncStateDone.set(false);
        return true;
    }

    private Tuple2<MapEntries, Integer> getAndClearEntries(int partitionId) {
        Tuple2<MapEntries, Integer> pair = Tuple2.tuple2(this.tempEntries[partitionId], partitionId);
        this.tempEntries[partitionId] = freshEntries();
        return pair;
    }

    private void copyWithoutHeader(Data src, byte[] dst, int dstOffset) {
        byte[] bytes = src.toByteArray();
        System.arraycopy(bytes, HeapData.TYPE_OFFSET, dst, dstOffset, bytes.length - HeapData.TYPE_OFFSET);
    }

    private void writeWithoutHeader(Data src, OutputStream dst) {
        byte[] bytes = src.toByteArray();
        try {
            dst.write(bytes, HeapData.TYPE_OFFSET, bytes.length - HeapData.TYPE_OFFSET);
        } catch (IOException e) {
            throw new RuntimeException(e); // should never happen
        }
    }

    @CheckReturnValue
    private boolean flushPartition(int partitionId) {
        return containsOnlyHeader(buffers[partitionId])
                || putAsyncToMap(partitionId, () -> getBufferContentsAndClear(buffers[partitionId]));
    }

    private boolean containsOnlyHeader(CustomByteArrayOutputStream buffer) {
        return buffer.size() == serializedByteArrayHeader.length;
    }

    private Data getBufferContentsAndClear(CustomByteArrayOutputStream buffer) {
        buffer.write(valueTerminator, 0, valueTerminator.length);
        final byte[] data = buffer.toByteArray();
        updateSerializedBytesLength(data);
        buffer.reset();
        buffer.write(serializedByteArrayHeader, 0, serializedByteArrayHeader.length);
        return new HeapData(data);
    }

    private void updateSerializedBytesLength(byte[] data) {
        // update the array length at the beginning of the buffer
        // the length is the third int value in the serialized data
        Bits.writeInt(data, 2 * Bits.INT_SIZE_IN_BYTES, data.length - serializedByteArrayHeader.length, useBigEndian);
    }

    @CheckReturnValue
    private boolean putAsyncToMap(int partitionId, Supplier<Data> dataSupplier) {
        if (!initCurrentMap()) {
            return false;
        }

        if (!Util.tryIncrement(numConcurrentAsyncOps, 1, JetService.MAX_PARALLEL_ASYNC_OPS)) {
            return false;
        }
        try {
            // we put a Data instance to the map directly to avoid the serialization of the byte array
            Data data = dataSupplier.get();
            totalPayloadBytes += data.dataSize();
            totalChunks++;
            CompletableFuture<Object> future = currentMap.putAsync(
                    new SnapshotDataKey(partitionKeys[partitionId], currentSnapshotId, vertexName, partitionSequence),
                    data).toCompletableFuture();
            partitionSequence += memberCount;
            future.whenComplete(putResponseConsumer);
            numActiveFlushes.incrementAndGet();
        } catch (HazelcastInstanceNotActiveException ignored) {
            return false;
        }
        return true;
    }

    /**
     * Helper for locking the temp state map lock.
     *
     * @return true if we got the lock, false otherwise
     */
    @CheckReturnValue
    private boolean lockStateMap() {
        return tempStateMapLock.compareAndSet(false, true);
    }

    /**
     * Helper for unlocking the temp state map lock.
     */
    private void unlockStateMap() {
        tempStateMapLock.set(false);
    }

    /**
     * Flushes the cache state map to the actual snapshot state IMap
     *
     * @return true if async operation started, false otherwise
     */
    @CheckReturnValue
    private boolean flushStateMap() {
        if (IMapStateHelper.isBatchPhaseStateEnabled(jetService.getConfig())) {
            // If batch enabled this is where we put it to the state map, clear the temp map afterwards
            int amountInTempState = 0;
            if (IMapStateHelper.isBatchPhaseConcurrentEnabled(jetService.getConfig())) {
                amountInTempState = tempStateMapConc.size();
            } else if (!IMapStateHelper.isFastSnapshotEnabled(jetService.getConfig())) {
                amountInTempState = tempStateMap.size();
            } else {
                for (MapEntries e : tempEntries) {
                    amountInTempState += e.size();
                }
            }
            if (amountInTempState == 0) {
                // Nothing in temp state map so we are done
                return true;
            }
            if (!IMapStateHelper.isBatchPhaseConcurrentEnabled(jetService.getConfig()) &&
                    !IMapStateHelper.isFastSnapshotEnabled(jetService.getConfig())) {
                boolean gotLock = lockStateMap();
                if (!gotLock) {
                    logger.fine("Couldn't get lock for setAllAsync");
                    // Return no progress if we didn't get the lock
                    return false;
                }
            }
            if (!Util.tryIncrement(numConcurrentAsyncOps, 1, JetService.MAX_PARALLEL_ASYNC_OPS)) {
                logger.info("Couldn't get async op for setAllAsync");
                return false;
            }

            logger.fine(String.format("Putting %d items to phase state map", amountInTempState));
            if (!IMapStateHelper.isFastSnapshotEnabled(jetService.getConfig())) {
                removeOldSnapshotIds();
                long beforeSetAll = System.nanoTime();
                stateMap.setAllAsync(IMapStateHelper.isBatchPhaseConcurrentEnabled(jetService.getConfig()) ?
                        tempStateMapConc :
                        tempStateMap)
                        .thenRun(() -> {
                            if (IMapStateHelper.isBatchPhaseConcurrentEnabled(jetService.getConfig())) {
                                tempStateMapConc.clear();
                            } else {
                                tempStateMap.clear();
                                logger.fine("Releasing lock from setAllAsync");
                                unlockStateMap();
                            }
                            if (IMapStateHelper.isDebugSnapshot(jetService.getConfig())) {
                                long afterSetAll = System.nanoTime();
                                logger.info(String.format("setAllAsync took: %d", afterSetAll - beforeSetAll));
                            }
                        }).toCompletableFuture().whenComplete(putResponseConsumer);
            } else {
                MapEntries[] setEntries = new MapEntries[tempEntries.length];
                int[] partitionIds = new int[tempEntries.length];
                int size = 0;
                for (int i = 0; i < tempEntries.length; i++) {
                    Tuple2<MapEntries, Integer> entriesAndPartitionId = getAndClearEntries(i);
                    if (entriesAndPartitionId.f0() == null ||
                            entriesAndPartitionId.f1() == null ||
                            entriesAndPartitionId.f0().size() == 0) {
                        continue;
                    }
                    setEntries[size] = entriesAndPartitionId.f0();
                    partitionIds[size] = entriesAndPartitionId.f1();
                    size++;
                }
                setEntries = Arrays.copyOf(setEntries, size);
                partitionIds = Arrays.copyOf(partitionIds, size);
                removeOldSnapshotIds();
                long beforeSetAllLocal = System.nanoTime();
                ((MapProxyImpl<SnapshotIMapKey<Object>, Object>) stateMap).setLocalEntriesAsync(partitionIds, setEntries)
                        .thenRun(() -> {
                            initTempEntries();
                            long afterSetAll = System.nanoTime();
                            if (IMapStateHelper.isDebugSnapshot(jetService.getConfig())) {
                                logger.info(String.format("setEntriesAsync took: %d", afterSetAll - beforeSetAllLocal));
                            }
                        }).toCompletableFuture().whenComplete(putResponseConsumer);
            }
            numActiveFlushes.incrementAndGet();
        }
        // We are done as batch setAllAsync has started
        return true;
    }

    private void removeIncrementalSnapshotSerially() {
        long beforeRemoveAll = System.nanoTime();
        long toRemoveAmount = snapshotsToRemoveList.size();
        snapshotsToRemoveList.forEach(key -> stateMap.delete(key));
        snapshotsToRemoveList.clear();
        if (IMapStateHelper.isDebugSnapshot(jetService.getConfig())) {
            long afterRemoveAll = System.nanoTime();
            logger.info(String.format(
                    "Remove %d old incremental snapshots took: %d",
                    toRemoveAmount,  afterRemoveAll - beforeRemoveAll));
        }
    }

    private void removeOldSnapshotsAll() {
        try {
            long beforeRemoveAll = System.nanoTime();
            long toRemoveAmount = snapshotsToRemove.size();
            List<Integer> localPartitions = partitionService.getMemberPartitions(nodeEngine.getThisAddress());
            AtomicInteger counter = new AtomicInteger(localPartitions.size());
            InternalCompletableFuture<Void> removeFuture = new InternalCompletableFuture<>();
            BiConsumer<Void, Throwable> callback = (response, t) -> {
                if (t != null) {
                    removeFuture.completeExceptionally(t);
                }
                if (counter.decrementAndGet() == 0 && !removeFuture.isDone()) {
                    removeFuture.complete(null);
                }
            };
            for (Integer i : localPartitions) {
                CompletableFuture.runAsync(() -> {
                    long beforeRemove = System.nanoTime();
                    Predicate<SnapshotIMapKey<Object>, Object> removePredicate;
                    if (IMapStateHelper.isIncrementalSnapshot(jetService.getConfig())
                            && IMapStateHelper.isIncrementalRemoveAll(jetService.getConfig())) {
                        // If incremental snapshots, use the HashSet predicate
                        removePredicate = IMapStateHelper.filterOldIncrementalSnapshots(
                                snapshotsToRemove, jetService.getConfig());
                    } else {
                        // If normal snapshots, just use current snapshot id predicate
                        removePredicate = IMapStateHelper.filterOldSnapshots(
                                currentSnapshotId, jetService.getConfig());
                    }
                    stateMap.removeAll(Predicates.partitionPredicate(
                            serializationService.toObject(partitionKey(i)),
                            removePredicate));
                    if (IMapStateHelper.isDebugRemove(jetService.getConfig())) {
                        long afterRemoveAllOnce = System.nanoTime();
                        logger.info(String.format("Remove all from %d took: %d",
                                i, afterRemoveAllOnce - beforeRemove));
                    }
                }).whenCompleteAsync(callback);
            }
            removeFuture.get(REMOVE_TIMEOUT, TimeUnit.SECONDS);
            snapshotsToRemove.clear();
            if (IMapStateHelper.isDebugSnapshot(jetService.getConfig())) {
                long afterRemoveAll = System.nanoTime();
                if (IMapStateHelper.isIncrementalRemoveAll(jetService.getConfig())) {
                    logger.info(String.format("Remove %d from all partitions took: %d",
                            toRemoveAmount, afterRemoveAll - beforeRemoveAll));
                } else {
                    logger.info(String.format("Remove from all partitions took: %d",
                            afterRemoveAll - beforeRemoveAll));
                }
            }
        } catch (Throwable e) {
            logger.severe("Got error during remove", e);
            throw rethrow(e);
        }
    }

    private void removeOldSnapshotIds() {
        if (!IMapStateHelper.isRemoveInMasterEnabled(jetService.getConfig())) {
            if (IMapStateHelper.isIncrementalSnapshot(jetService.getConfig())
                    && !IMapStateHelper.isIncrementalRemoveAll(jetService.getConfig())) {
                // Remove old snapshots one by one
                removeIncrementalSnapshotSerially();
            } else {
                // Remove old snapshots all at once
                removeOldSnapshotsAll();
            }
        }
    }

    /**
     * Handler for non timestamped item.
     *
     * @return true if handling was done, false otherwise
     */
    private boolean handleNonTimestamped() {
        // If it is not TimestampedItem it is most likely a watermark
        if (IMapStateHelper.isBatchPhaseStateWatermarkEnabled(jetService.getConfig())) {
            return flushStateMap();
        }
        return true;
    }

    @CheckReturnValue
    private boolean putAsyncToStateMap(Entry<? extends Data, ? extends Data> entry) {
        if (!initStateMap()) {
            return false;
        }
        if (!initTempEntries()) {
            return false;
        }
        try {
            if (entry.getValue().getType() != SerializerHookConstants.TIMESTAMPED_ITEM) {
                return handleNonTimestamped();
            }
            int partitionId = partitionService.getPartitionId(entry.getKey());
            // Serialized form of item inside the TimestampedItem
            byte[] array = new byte[entry.getValue().totalSize() - Long.BYTES];
            Bits.writeInt(array, 0, partitionKey(partitionId), true);
            System.arraycopy(entry.getValue().toByteArray(),
                    HeapData.HEAP_DATA_OVERHEAD + Long.BYTES,
                    array,
                    Integer.BYTES,
                    entry.getValue().totalSize() - HeapData.HEAP_DATA_OVERHEAD - Long.BYTES);
            Data valueItemData = new HeapData(array); // Data form of item inside TimestampedItem
            currentSnapshotId = snapshotContext.currentSnapshotId();
            if (IMapStateHelper.isBatchPhaseStateEnabled(jetService.getConfig())) {
                if (IMapStateHelper.isBatchPhaseConcurrentEnabled(jetService.getConfig())) {
                    Object key = serializationService.toObject(entry.getKey());
                    SnapshotIMapKey<Object> ssKey = new SnapshotIMapKey<>(key, currentSnapshotId);
                    tempStateMapConc.put(ssKey, valueItemData);
                } else if (!IMapStateHelper.isFastSnapshotEnabled(jetService.getConfig())) {
                    Object key = serializationService.toObject(entry.getKey());
                    SnapshotIMapKey<Object> ssKey = new SnapshotIMapKey<>(key, currentSnapshotId);
                    // If batch mode, put to internal map
                    boolean gotLock = lockStateMap();
                    if (!gotLock) {
                        logger.fine("Couldn't get lock for put to temp");
                        // Return no progress if we didn't get the lock
                        return false;
                    }
                    tempStateMap.put(ssKey, valueItemData);
                    unlockStateMap();
                } else {
                    Data ssKey = SnapshotIMapKey.fromData(entry.getKey(), currentSnapshotId);
                    tempEntries[partitionId].add(ssKey, valueItemData);
                }
            } else {
                if (!Util.tryIncrement(numConcurrentAsyncOps, 1, JetService.MAX_PARALLEL_ASYNC_OPS)) {
                    logger.fine("Couldn't get async op for setAsync");
                    return false;
                }
                Object key = serializationService.toObject(entry.getKey());
                SnapshotIMapKey<Object> ssKey = new SnapshotIMapKey<>(key, currentSnapshotId);
                // Otherwise put to state map immediately
                stateMap.setAsync(ssKey, valueItemData)
                        .toCompletableFuture().whenComplete(putResponseConsumer);
                numActiveFlushes.incrementAndGet();
            }
        } catch (HazelcastInstanceNotActiveException ignored) {
            return false;
        }
        // Done if no incremental snapshot configured
        if (!IMapStateHelper.isIncrementalSnapshot(jetService.getConfig())) {
            return true;
        }
        // Put key to prev entries and update snapshots to remove array/set
        Object key = serializationService.toObject(entry.getKey());
        prevSnapshotIdMap.putIfAbsent(key,
                new ArrayDeque<>(IMapStateHelper.getSnapshotsToKeep(jetService.getConfig())));
        ArrayDeque<Long> snapshotIds = prevSnapshotIdMap.get(key);
        snapshotIds.offerLast(currentSnapshotId);
        while (snapshotIds.size() > IMapStateHelper.getSnapshotsToKeep(jetService.getConfig())) {
            Long oldestSnapshotId = snapshotIds.pollFirst();
            if (oldestSnapshotId == null) {
                throw new IllegalStateException("No available snapshot ID in deque!");
            }
            if (IMapStateHelper.isIncrementalRemoveAll(jetService.getConfig())) {
                snapshotsToRemove.add(new SnapshotIMapKey<>(key, oldestSnapshotId));
            } else {
                snapshotsToRemoveList.add(new SnapshotIMapKey<>(key, oldestSnapshotId));
            }
        }
        return true;
    }

    private boolean initCurrentMap() {
        if (currentMap == null) {
            String mapName = snapshotContext.currentMapName();
            if (mapName == null) {
                return false;
            }
            if (!nodeEngine.getHazelcastInstance().getConfig().getMapConfigs().containsKey(mapName)) {
                MapConfig mapConfig = new MapConfig(mapName);
                nodeEngine.getHazelcastInstance().getConfig().addMapConfig(mapConfig);
                if (IMapStateHelper.isMemoryFormatObject(jetService.getConfig())) {
                    mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
                }
            }
            currentMap = nodeEngine.getHazelcastInstance().getMap(mapName);
            this.currentSnapshotId = snapshotContext.currentSnapshotId();
        }
        return true;
    }

    private boolean initStateMap() {
        if (stateMap == null) {
            String mapName = IMapStateHelper.getPhaseSnapshotMapName(vertexName);
            MapConfig mapConfig = new MapConfig(mapName);
            nodeEngine.getHazelcastInstance().getConfig().addMapConfig(mapConfig);
            if (!IMapStateHelper.enableBackup(jetService.getConfig())) {
                mapConfig.setBackupCount(0);
                mapConfig.setAsyncBackupCount(0);
            }
            mapConfig.setStatisticsEnabled(IMapStateHelper.isStatisticsEnabled(jetService.getConfig()));
            if (IMapStateHelper.isMemoryFormatObject(jetService.getConfig())) {
                mapConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
            }
            stateMap = nodeEngine.getHazelcastInstance().getMap(mapName);
            this.currentSnapshotId = snapshotContext.currentSnapshotId();
        }
        return true;
    }

    /**
     * Must be called AFTER initStateMap().
     * @return True if initialized, false otherwise
     */
    private boolean initTempEntries() {
        // Fast snapshot mode using setEntries()
        if (tempEntries == null) {
            int partitionCount = partitionService.getPartitionCount();
            tempEntries = new MapEntries[partitionCount];
        }
        for (int i = 0; i < tempEntries.length; i++) {
            MapEntries entries = tempEntries[i];
            if (entries == null) {
                entries = freshEntries();
                tempEntries[i] = entries;
            }
        }
        return true;
    }

    /**
     * Helper method for a new MapEntries object.
     * @return Empty MapEntries object
     */
    private MapEntries freshEntries() {
        int initialSize = ((MapProxyImpl<SnapshotIMapKey<Object>, Object>) stateMap)
                .getPutAllInitialSize(false, MAP_SIZE, tempEntries.length);
        return new MapEntries(initialSize);
    }

    /**
     * Flush all partitions and reset current map. No further items can be
     * offered until new snapshot is seen in {@link #snapshotContext}.
     *
     * @return {@code true} on success, {@code false} if we weren't able to
     * flush some partitions due to the limit on number of parallel async ops.
     */
    @Override
    @CheckReturnValue
    public boolean flushAndResetMap() {
        if (!initCurrentMap()) {
            return false;
        }

        for (int i = 0; i < buffers.length; i++) {
            if (!flushPartition(i)) {
                return false;
            }
        }

        if (!initStateMap()) {
            return false;
        }

        if (!initTempEntries()) {
            return false;
        }

        // Flush state map
        if (!flushStateMap()) {
            return false;
        }

        // we're done
        currentMap = null;
        if (logger.isFineEnabled()) {
            logger.fine(String.format("Stats for %s: keys=%,d, chunks=%,d, bytes=%,d",
                    vertexName, totalKeys, totalChunks, totalPayloadBytes));
        }
        return true;
    }

    @Override
    public void resetStats() {
        totalKeys = totalChunks = totalPayloadBytes = 0;
    }

    @Override
    public boolean hasPendingAsyncOps() {
        return numActiveFlushes.get() > 0;
    }

    @Override
    public Throwable getError() {
        return firstError.getAndSet(null);
    }

    @Override
    public boolean isEmpty() {
        return numActiveFlushes.get() == 0 && Arrays.stream(buffers).allMatch(this::containsOnlyHeader);
    }

    int partitionKey(int partitionId) {
        return partitionKeys[partitionId];
    }

    @Override
    public long getTotalPayloadBytes() {
        return totalPayloadBytes;
    }

    @Override
    public long getTotalKeys() {
        return totalKeys;
    }

    @Override
    public long getTotalChunks() {
        return totalChunks;
    }

    public static final class SnapshotDataKey implements IdentifiedDataSerializable, PartitionAware {
        private int partitionKey;
        private long snapshotId;
        private String vertexName;
        private int sequence;

        // for deserialization
        public SnapshotDataKey() {
        }

        public SnapshotDataKey(int partitionKey, long snapshotId, String vertexName, int sequence) {
            this.partitionKey = partitionKey;
            this.snapshotId = snapshotId;
            this.vertexName = vertexName;
            this.sequence = sequence;
        }

        @Override
        public Object getPartitionKey() {
            return partitionKey;
        }

        public long snapshotId() {
            return snapshotId;
        }

        public String vertexName() {
            return vertexName;
        }

        @Override
        public String toString() {
            return "SnapshotDataKey{" +
                    "partitionKey=" + partitionKey +
                    ", snapshotId=" + snapshotId +
                    ", vertexName='" + vertexName + '\'' +
                    ", sequence=" + sequence +
                    '}';
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetInitDataSerializerHook.ASYNC_SNAPSHOT_WRITER_SNAPSHOT_DATA_KEY;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(partitionKey);
            out.writeLong(snapshotId);
            out.writeUTF(vertexName);
            out.writeInt(sequence);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            partitionKey = in.readInt();
            snapshotId = in.readLong();
            vertexName = in.readUTF();
            sequence = in.readInt();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SnapshotDataKey that = (SnapshotDataKey) o;
            return partitionKey == that.partitionKey &&
                    snapshotId == that.snapshotId &&
                    sequence == that.sequence &&
                    Objects.equals(vertexName, that.vertexName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionKey, snapshotId, vertexName, sequence);
        }
    }

    public static final class SnapshotDataValueTerminator implements IdentifiedDataSerializable {

        public static final IdentifiedDataSerializable INSTANCE = new SnapshotDataValueTerminator();

        private SnapshotDataValueTerminator() {
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetInitDataSerializerHook.ASYNC_SNAPSHOT_WRITER_SNAPSHOT_DATA_VALUE_TERMINATOR;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
        }

        @Override
        public void readData(ObjectDataInput in) {
        }
    }

    /**
     * Non-synchronized variant of {@code java.io.ByteArrayOutputStream} with capacity limit.
     */
    static class CustomByteArrayOutputStream extends OutputStream {

        private static final byte[] EMPTY_BYTE_ARRAY = {};
        private final int capacityLimit;
        private byte[] data;
        private int size;

        CustomByteArrayOutputStream(int capacityLimit) {
            this.capacityLimit = capacityLimit;
            // initial capacity is 0. It will take several reallocations to reach typical capacity,
            // but it's also common to remain at 0 - for partitions not assigned to us.
            data = EMPTY_BYTE_ARRAY;
        }

        @Override
        public void write(int b) {
            ensureCapacity(size + 1);
            data[size] = (byte) b;
            size++;
        }

        public void write(@Nonnull byte[] b, int off, int len) {
            if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) - b.length > 0)) {
                throw new IndexOutOfBoundsException("off=" + off + ", len=" + len);
            }
            ensureCapacity(size + len);
            System.arraycopy(b, off, data, size, len);
            size += len;
        }

        private void ensureCapacity(int minCapacity) {
            if (minCapacity - data.length > 0) {
                int newCapacity = data.length;
                do {
                    newCapacity = Math.max(1, newCapacity << 1);
                } while (newCapacity - minCapacity < 0);
                if (newCapacity - capacityLimit > 0) {
                    throw new IllegalStateException("buffer full");
                }
                data = Arrays.copyOf(data, newCapacity);
            }
        }

        void reset() {
            size = 0;
        }

        @Nonnull
        byte[] toByteArray() {
            return Arrays.copyOf(data, size);
        }

        int size() {
            return size;
        }
    }
}
