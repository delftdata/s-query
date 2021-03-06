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

package com.hazelcast.sql.impl.expression.aggregate;

import com.hazelcast.sql.impl.exec.agg.AggregateCollector;
import com.hazelcast.sql.impl.type.QueryDataType;

/**
 * Counting collector.
 */
public final class CountAggregateCollector extends AggregateCollector {
    /** Final result. */
    private long res;

    public CountAggregateCollector(boolean distinct) {
        super(distinct);
    }

    @Override
    protected void collect0(Object operandValue, QueryDataType operandType) {
        collectMany(1);
    }

    public void collectMany(long cnt) {
        res += cnt;
    }

    @Override
    public Object reduce() {
        return res;
    }
}
