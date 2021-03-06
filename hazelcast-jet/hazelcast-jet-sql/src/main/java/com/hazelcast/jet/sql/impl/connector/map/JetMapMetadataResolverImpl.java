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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataJavaResolver;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.sql.impl.schema.map.JetMapMetadataResolver;

import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public final class JetMapMetadataResolverImpl implements JetMapMetadataResolver {

    public static final JetMapMetadataResolverImpl INSTANCE = new JetMapMetadataResolverImpl();

    private JetMapMetadataResolverImpl() {
    }

    @Override
    public Object resolveClass(Class<?> clazz, boolean key) {
        List<MappingField> mappingFields = KvMetadataJavaResolver.INSTANCE.resolveFields(key, emptyList(), clazz);
        KvMetadata metadata = KvMetadataJavaResolver.INSTANCE.resolveMetadata(key, mappingFields, clazz);
        return metadata.getUpsertTargetDescriptor();
    }

    @Override
    public Object resolvePortable(ClassDefinition clazz, boolean key) {
        List<MappingField> mappingFields = MetadataPortableResolver.INSTANCE.resolveFields(key, emptyList(), clazz);
        KvMetadata metadata = MetadataPortableResolver.INSTANCE.resolveMetadata(key, mappingFields, clazz);
        return metadata.getUpsertTargetDescriptor();
    }

    @Override
    public Object resolveJson(boolean key) {
        List<MappingField> mappingFields = MetadataJsonResolver.INSTANCE.resolveAndValidateFields(
                key, emptyList(), emptyMap(), null);
        KvMetadata metadata = MetadataJsonResolver.INSTANCE.resolveMetadata(
                key, mappingFields, emptyMap(), null);
        return metadata.getUpsertTargetDescriptor();
    }
}
