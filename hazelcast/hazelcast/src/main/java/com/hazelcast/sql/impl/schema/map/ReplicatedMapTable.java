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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.plan.cache.PlanObjectKey;
import com.hazelcast.sql.impl.plan.cache.ReplicatedMapPlanObjectKey;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.TableStatistics;

import java.util.List;

import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_REPLICATED;

public class ReplicatedMapTable extends AbstractMapTable {

    public ReplicatedMapTable(
            String schemaName,
            String tableName,
            String mapName,
            List<TableField> fields,
            TableStatistics statistics,
            QueryTargetDescriptor keyDescriptor,
            QueryTargetDescriptor valueDescriptor,
            Object keyJetMetadata,
            Object valueJetMetadata
    ) {
        super(schemaName, tableName, mapName, fields, statistics, keyDescriptor, valueDescriptor,
                keyJetMetadata, valueJetMetadata);
    }

    public ReplicatedMapTable(String name, QueryException exception) {
        super(SCHEMA_NAME_REPLICATED, name, exception);
    }

    @Override
    public PlanObjectKey getObjectKey() {
        if (!isValid()) {
            return null;
        }

        return new ReplicatedMapPlanObjectKey(
            getSchemaName(),
            getMapName(),
            getFields(),
            getConflictingSchemas(),
            getKeyDescriptor(),
            getValueDescriptor()
        );
    }
}
