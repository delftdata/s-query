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

package com.hazelcast.sql.impl.exec;

import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.sql.impl.row.RowBatch;

/**
 * Basic execution stage.
 */
public interface Exec {
    /**
     * @return ID of the executor, which uniquely identifies the executor within the query.
     */
    int getId();

    /**
     * One-time setup of the executor.
     *
     * @param ctx Context.
     */
    void setup(QueryFragmentContext ctx);

    /**
     * Try advancing executor. Content of the current batch will be changed as a result of this call.
     *
     * @return Result of iteration.
     */
    IterationResult advance();

    /**
     * @return Current batch available in response to the previous {@link #advance()} call. Should never be null.
     */
    RowBatch currentBatch();

    /**
     * @return {@code true} if the input could be reset.
     */
    boolean canReset();

    /**
     * Perform the reset.
     */
    void reset();
}
