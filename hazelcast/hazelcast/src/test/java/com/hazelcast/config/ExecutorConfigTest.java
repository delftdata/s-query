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

package com.hazelcast.config;

import com.hazelcast.internal.config.ExecutorConfigReadOnly;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assumeDifferentHashCodes;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExecutorConfigTest {

    @Test
    public void testGetCorePoolSize() {
        ExecutorConfig executorConfig = new ExecutorConfig();
        assertTrue(executorConfig.getPoolSize() == ExecutorConfig.DEFAULT_POOL_SIZE);
    }

    @Test
    public void testSetCorePoolSize() {
        ExecutorConfig executorConfig = new ExecutorConfig().setPoolSize(1234);
        assertTrue(executorConfig.getPoolSize() == 1234);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAcceptNegativeCorePoolSize() {
        new ExecutorConfig().setPoolSize(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAcceptZeroCorePoolSize() {
        new ExecutorConfig().setPoolSize(0);
    }

    @Test
    public void testEqualsAndHashCode() {
        assumeDifferentHashCodes();
        EqualsVerifier.forClass(ExecutorConfig.class)
                .suppress(Warning.NULL_FIELDS, Warning.NONFINAL_FIELDS)
                .withPrefabValues(ExecutorConfigReadOnly.class,
                        new ExecutorConfigReadOnly(new ExecutorConfig("red")),
                        new ExecutorConfigReadOnly(new ExecutorConfig("black")))
                .verify();
    }
}