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

package com.hazelcast.jet.sql.impl.connector.test;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.pipeline.transform.BatchSourceTransform;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.Util.toList;
import static java.time.ZoneOffset.UTC;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * A SQL source yielding a single row with all supported types.
 */
public class AllTypesSqlConnector implements SqlConnector {

    public static final String TYPE_NAME = "AllTypes";

    private static final List<MappingField> FIELD_LIST = asList(
            new MappingField("string", QueryDataType.VARCHAR),
            new MappingField("boolean", QueryDataType.BOOLEAN),
            new MappingField("byte", QueryDataType.TINYINT),
            new MappingField("short", QueryDataType.SMALLINT),
            new MappingField("int", QueryDataType.INT),
            new MappingField("long", QueryDataType.BIGINT),
            new MappingField("float", QueryDataType.REAL),
            new MappingField("double", QueryDataType.DOUBLE),
            new MappingField("decimal", QueryDataType.DECIMAL),
            new MappingField("time", QueryDataType.TIME),
            new MappingField("date", QueryDataType.DATE),
            new MappingField("timestamp", QueryDataType.TIMESTAMP),
            new MappingField("timestampTz", QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME),
            new MappingField("object", QueryDataType.OBJECT)

    );
    private static final List<TableField> FIELD_LIST2 = toList(FIELD_LIST, f -> new TableField(f.name(), f.type(), false));

    private static final Object[] VALUES = new Object[]{
            "string",
            true,
            (byte) 127,
            (short) 32767,
            2147483647,
            9223372036854775807L,
            1234567890.1f,
            123451234567890.1,
            new BigDecimal("9223372036854775.123"),
            LocalTime.of(12, 23, 34),
            LocalDate.of(2020, 4, 15),
            LocalDateTime.of(2020, 4, 15, 12, 23, 34, 1_000_000),
            OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC),
            null
    };

    public static void create(SqlService sqlService, String tableName) {
        sqlService.execute("CREATE MAPPING " + tableName + " TYPE " + AllTypesSqlConnector.TYPE_NAME);
    }

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Override
    public boolean isStream() {
        return false;
    }

    @Nonnull @Override
    public List<MappingField> resolveAndValidateFields(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> userFields
    ) {
        if (userFields.size() > 0) {
            throw QueryException.error("Don't specify external fields, they are fixed");
        }
        return FIELD_LIST;
    }

    @Nonnull @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mappingName,
            @Nonnull String externalName,
            @Nonnull Map<String, String> options,
            @Nonnull List<MappingField> resolvedFields
    ) {
        return new AllTypesTable(this, schemaName, mappingName);
    }

    @Override
    public boolean supportsFullScanReader() {
        return true;
    }

    @Nonnull @Override
    public Vertex fullScanReader(
            @Nonnull DAG dag,
            @Nonnull Table table,
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projection
    ) {
        Object[] row = ExpressionUtil.evaluate(predicate, projection, VALUES);
        BatchSource<Object[]> source = TestSources.items(singletonList(row));
        ProcessorMetaSupplier pms = ((BatchSourceTransform<Object[]>) source).metaSupplier;
        return dag.newUniqueVertex(table.toString(), pms);
    }

    public static class AllTypesTable extends JetTable {

        public AllTypesTable(
                @Nonnull SqlConnector sqlConnector,
                @Nonnull String schemaName,
                @Nonnull String name
        ) {
            super(sqlConnector, FIELD_LIST2, schemaName, name, new ConstantTableStatistics(1));
        }

        @Override
        public String toString() {
            return "AllTypes" + "[" + getSchemaName() + "." + getSqlName() + "]";
        }
    }
}
