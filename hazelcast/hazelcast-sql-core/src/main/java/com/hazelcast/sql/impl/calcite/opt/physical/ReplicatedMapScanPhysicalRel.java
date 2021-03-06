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

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PhysicalRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * Physical scan over replicated map.
 * <p>
 * Traits:
 * <ul>
 *     <li><b>Collation</b>: empty, as map is not sorted</li>
 *     <li><b>Distribution</b>: REPLICATED</li>
 * </ul>
 */
public class ReplicatedMapScanPhysicalRel extends AbstractMapScanPhysicalRel {
    public ReplicatedMapScanPhysicalRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table
    ) {
        super(cluster, traitSet, table);
    }

    @Override
    public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new ReplicatedMapScanPhysicalRel(getCluster(), traitSet, getTable());
    }

    @Override
    public void visit(PhysicalRelVisitor visitor) {
        visitor.onReplicatedMapScan(this);
    }
}
