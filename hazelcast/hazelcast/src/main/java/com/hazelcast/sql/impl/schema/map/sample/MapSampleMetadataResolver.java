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

package com.hazelcast.sql.impl.schema.map.sample;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.sql.impl.FieldsUtil;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.JetMapMetadataResolver;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * Helper class that resolves a map-backed table from a key/value sample.
 */
// TODO: deduplicate with MapOptionsMetadataResolvers
public final class MapSampleMetadataResolver {

    private MapSampleMetadataResolver() {
        // No-op.
    }

    /**
     * Resolves the metadata associated with the given key-value sample.
     *
     * @param ss Serialization service.
     * @param target Target to be analyzed.
     * @param key Whether passed target is key or value.
     * @return Sample metadata.
     * @throws QueryException If metadata cannot be resolved.
     */
    public static MapSampleMetadata resolve(
        InternalSerializationService ss,
        JetMapMetadataResolver jetMapMetadataResolver,
        Object target,
        boolean key
    ) {
        try {
            // Convert Portable object to Data to have consistent object fields irrespectively of map's InMemoryFormat.
            if (target instanceof Portable) {
                target = ss.toData(target);
            }

            if (target instanceof Data) {
                Data data = (Data) target;

                if (data.isPortable()) {
                    return resolvePortable(ss.getPortableContext().lookupClassDefinition(data), key, jetMapMetadataResolver);
                } else if (data.isJson()) {
                    return resolveJson(ss.toObject(data), key, jetMapMetadataResolver);
                } else {
                    return resolveClass(ss.toObject(data).getClass(), key, jetMapMetadataResolver);
                }
            } else {
                return resolveClass(target.getClass(), key, jetMapMetadataResolver);
            }
        } catch (Exception e) {
            throw QueryException.error("Failed to resolve " + (key ? "key" : "value") + " metadata: " + e.getMessage(), e);
        }
    }

    /**
     * Resolve metadata from a portable object.
     *
     * @param clazz Portable class definition.
     * @param isKey Whether this is a key.
     * @return Metadata.
     */
    private static MapSampleMetadata resolvePortable(
        ClassDefinition clazz,
        boolean isKey,
        JetMapMetadataResolver jetMapMetadataResolver
    ) {
        LinkedHashMap<String, TableField> fields = new LinkedHashMap<>();

        Map<String, QueryDataType> simpleFields = FieldsUtil.resolvePortable(clazz);

        for (Entry<String, QueryDataType> fieldEntry : simpleFields.entrySet()) {
            String name = fieldEntry.getKey();
            TableField oldValue = fields.put(name,
                    new MapTableField(name, fieldEntry.getValue(), false, new QueryPath(name, isKey)));
            assert oldValue == null;
        }

        // Add top-level object.
        String topName = isKey ? QueryPath.KEY : QueryPath.VALUE;
        QueryPath topPath = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;
        // explicitly remove to have the newly-inserted topName at the end
        fields.remove(topName);
        fields.put(topName, new MapTableField(topName, QueryDataType.OBJECT, !fields.isEmpty(), topPath));

        return new MapSampleMetadata(
            GenericQueryTargetDescriptor.DEFAULT,
            jetMapMetadataResolver.resolvePortable(clazz, isKey),
            new LinkedHashMap<>(fields)
        );
    }

    private static MapSampleMetadata resolveClass(
        Class<?> clazz,
        boolean isKey,
        JetMapMetadataResolver jetMapMetadataResolver
    ) {
        LinkedHashMap<String, TableField> fields = new LinkedHashMap<>();

        // Extract fields from non-primitive type.
        QueryDataType topType = QueryDataTypeUtils.resolveTypeForClass(clazz);

        if (topType == QueryDataType.OBJECT) {
            Map<String, Class<?>> simpleFields = FieldsUtil.resolveClass(clazz);

            for (Entry<String, Class<?>> fieldEntry : simpleFields.entrySet()) {
                String fieldName = fieldEntry.getKey();
                QueryDataType type = QueryDataTypeUtils.resolveTypeForClass(fieldEntry.getValue());
                TableField oldValue = fields.put(fieldName,
                        new MapTableField(fieldName, type, false, new QueryPath(fieldName, isKey)));
                assert oldValue == null;
            }
        }

        // Add top-level object.
        String topName = isKey ? QueryPath.KEY : QueryPath.VALUE;
        QueryPath topPath = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;
        // explicitly remove to have the newly-inserted topName at the end
        fields.remove(topName);
        fields.put(topName, new MapTableField(topName, topType, !fields.isEmpty(), topPath));

        return new MapSampleMetadata(
            GenericQueryTargetDescriptor.DEFAULT,
            jetMapMetadataResolver.resolveClass(clazz, isKey),
            fields
        );
    }

    private static MapSampleMetadata resolveJson(
        HazelcastJsonValue json,
        boolean isKey,
        JetMapMetadataResolver jetMapMetadataResolver
    ) {
        Map<String, TableField> fields = new TreeMap<>();
        Set<String> pathsRequiringConversion = new HashSet<>();

        // Add regular fields.
        JsonObject object = Json.parse(json.toString()).asObject();
        for (JsonObject.Member member : object) {
            QueryPath path = new QueryPath(member.getName(), isKey);
            QueryDataType type = resolveJsonType(member.getValue());
            String name = member.getName();
            boolean requiresConversion = doesRequireConversion(type);

            MapTableField field = new MapTableField(name, type, false, path, requiresConversion);

            if (fields.putIfAbsent(field.getName(), field) == null && field.isRequiringConversion()) {
                pathsRequiringConversion.add(field.getPath().getPath());
            }
        }

        // Add top-level object.
        fields = new LinkedHashMap<>(fields);

        String topName = isKey ? QueryPath.KEY : QueryPath.VALUE;
        QueryPath topPath = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;

        fields.remove(topName);
        fields.put(topName, new MapTableField(topName, QueryDataType.OBJECT, !fields.isEmpty(), topPath));

        return new MapSampleMetadata(
            new GenericQueryTargetDescriptor(pathsRequiringConversion),
            jetMapMetadataResolver.resolveJson(isKey),
            new LinkedHashMap<>(fields)
        );
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private static QueryDataType resolveJsonType(JsonValue value) {
        if (value.isBoolean()) {
            return QueryDataType.BOOLEAN;
        } else if (value.isNumber()) {
            return QueryDataType.DOUBLE;
        } else if (value.isString()) {
            return QueryDataType.VARCHAR;
        } else {
            return QueryDataType.OBJECT;
        }
    }

    private static boolean doesRequireConversion(QueryDataType type) {
        switch (type.getTypeFamily()) {
            case BOOLEAN:
                // case BIGINT: 1.0 is being stored as 1 leading effectively to reading value of type Long
            case VARCHAR:
                return false;
            default:
                return true;
        }
    }
}
