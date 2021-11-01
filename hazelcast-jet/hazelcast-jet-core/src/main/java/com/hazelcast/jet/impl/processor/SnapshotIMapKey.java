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

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.PartitionAware;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

import static com.hazelcast.internal.serialization.impl.HeapData.HEAP_DATA_OVERHEAD;
import static com.hazelcast.internal.serialization.impl.HeapData.TYPE_OFFSET;
import static com.hazelcast.internal.serialization.impl.SerializationConstants.CONSTANT_TYPE_DATA_SERIALIZABLE;

public final class SnapshotIMapKey<K> implements IdentifiedDataSerializable, PartitionAware<K>, Serializable {
    private K key;


    private long snapshotId;

    // For deserialization
    public SnapshotIMapKey() {

    }

    public SnapshotIMapKey(K key, long snapshotId) {
        this.key = key;
        this.snapshotId = snapshotId;
    }

    public static Data fromData(Data key, long snapshotId) {
        // Get partition hash
        int partition = key.getPartitionHash();
        byte[] array = new byte[
                HEAP_DATA_OVERHEAD + // Data header
                        1 + Integer.BYTES + Integer.BYTES + // IdentifiedDataSerializable fields
                        key.totalSize() - TYPE_OFFSET + // Key Object
                        Long.BYTES]; // snapshot ID long
        int pos = 0;
        Bits.writeInt(array, pos, partition, true); // Partition ID
        pos += Integer.BYTES;
        Bits.writeInt(array, pos, CONSTANT_TYPE_DATA_SERIALIZABLE, true); // Type ID of DataSerializable
        pos += Integer.BYTES;
        array[pos] = 1; // Set Identified bit
        pos += 1;
        Bits.writeInt(array, pos, JetInitDataSerializerHook.FACTORY_ID, true); // Factory ID
        pos += Integer.BYTES;
        Bits.writeInt(array, pos, JetInitDataSerializerHook.SNAPSHOTIMAP_KEY, true); // Type ID again
        pos += Integer.BYTES;
        System.arraycopy(key.toByteArray(), TYPE_OFFSET, array, pos, key.totalSize() - TYPE_OFFSET);
        pos += key.totalSize() - TYPE_OFFSET;
        Bits.writeLong(array, pos, snapshotId, true);
        return new HeapData(array);
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    @Override
    public K getPartitionKey() {
        return key;
    }

    @Override
    public int getFactoryId() {
        return JetInitDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.SNAPSHOTIMAP_KEY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(key);
        out.writeLong(snapshotId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = in.readObject();
        snapshotId = in.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SnapshotIMapKey<?> that = (SnapshotIMapKey<?>) o;
        return snapshotId == that.snapshotId &&
                Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, snapshotId);
    }
}
