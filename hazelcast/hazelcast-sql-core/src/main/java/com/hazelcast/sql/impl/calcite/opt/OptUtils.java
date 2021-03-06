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

package com.hazelcast.sql.impl.calcite.opt;

import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTrait;
import com.hazelcast.sql.impl.calcite.opt.distribution.DistributionTraitDef;
import com.hazelcast.sql.impl.calcite.schema.HazelcastRelOptTable;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;

import static org.apache.calcite.plan.RelOptRule.convert;

/**
 * Utility methods for rules.
 */
public final class OptUtils {
    private OptUtils() {
        // No-op.
    }

    /**
     * Get operand matching a single node.
     *
     * @param cls Node class.
     * @param convention Convention.
     * @return Operand.
     */
    public static <R extends RelNode> RelOptRuleOperand single(Class<R> cls, Convention convention) {
        return RelOptRule.operand(cls, convention, RelOptRule.any());
    }

    /**
     * Get operand matching a node with specific child node.
     *
     * @param cls Node class.
     * @param childCls Child node class.
     * @param convention Convention.
     * @return Operand.
     */
    public static <R1 extends RelNode, R2 extends RelNode> RelOptRuleOperand parentChild(Class<R1> cls,
        Class<R2> childCls, Convention convention) {
        RelOptRuleOperand childOperand = RelOptRule.operand(childCls, RelOptRule.any());

        return RelOptRule.operand(cls, convention, RelOptRule.some(childOperand));
    }

    /**
     * Get operand matching a node with specific pair of child nodes.
     *
     * @param cls Node class.
     * @param childCls1 Child node class 1.
     * @param childCls2 Child node class 2.
     * @param convention Convention.
     * @return Operand.
     */
    public static <R1 extends RelNode, R2 extends RelNode> RelOptRuleOperand parentChildChild(
        Class<R1> cls,
        Class<R2> childCls1,
        Class<R2> childCls2,
        Convention convention
    ) {
        RelOptRuleOperand childOperand1 = RelOptRule.operand(childCls1, RelOptRule.any());
        RelOptRuleOperand childOperand2 = RelOptRule.operand(childCls2, RelOptRule.any());

        return RelOptRule.operand(cls, convention, RelOptRule.some(childOperand1, childOperand2));
    }

    /**
     * Add a single trait to the trait set.
     *
     * @param traitSet Original trait set.
     * @param trait Trait to add.
     * @return Resulting trait set.
     */
    public static RelTraitSet traitPlus(RelTraitSet traitSet, RelTrait trait) {
        return traitSet.plus(trait).simplify();
    }

    /**
     * Add two traits to the trait set.
     *
     * @param traitSet Original trait set.
     * @param trait1 Trait to add.
     * @param trait2 Trait to add.
     * @return Resulting trait set.
     */
    public static RelTraitSet traitPlus(RelTraitSet traitSet, RelTrait trait1, RelTrait trait2) {
        return traitSet.plus(trait1).plus(trait2).simplify();
    }

    /**
     * Add three traits to the trait set.
     *
     * @param traitSet Original trait set.
     * @param trait1 Trait to add.
     * @param trait2 Trait to add.
     * @param trait3 Trait to add.
     * @return Resulting trait set.
     */
    public static RelTraitSet traitPlus(RelTraitSet traitSet, RelTrait trait1, RelTrait trait2, RelTrait trait3) {
        return traitSet.plus(trait1).plus(trait2).plus(trait3).simplify();
    }

    /**
     * Convert the given trait set to logical convention.
     *
     * @param traitSet Original trait set.
     * @return New trait set with logical convention.
     */
    public static RelTraitSet toLogicalConvention(RelTraitSet traitSet) {
        return traitPlus(traitSet, HazelcastConventions.LOGICAL);
    }

    /**
     * Convert the given input into logical input.
     *
     * @param input Original input.
     * @return Logical input.
     */
    public static RelNode toLogicalInput(RelNode input) {
        return convert(input, toLogicalConvention(input.getTraitSet()));
    }

    /**
     * Convert the given trait set to physical convention.
     *
     * @param traitSet Original trait set.
     * @return New trait set with physical convention and provided distribution.
     */
    public static RelTraitSet toPhysicalConvention(RelTraitSet traitSet) {
        return traitPlus(traitSet, HazelcastConventions.PHYSICAL);
    }

    /**
     * Convert the given trait set to physical convention.
     *
     * @param traitSet Original trait set.
     * @param distribution Distribution.
     * @return New trait set with physical convention and provided distribution.
     */
    public static RelTraitSet toPhysicalConvention(RelTraitSet traitSet, DistributionTrait distribution) {
        return traitPlus(traitSet, HazelcastConventions.PHYSICAL, distribution);
    }

    public static RelTraitSet toPhysicalConvention(RelTraitSet traitSet, DistributionTrait distribution,
        RelCollation collation) {
        return traitPlus(traitSet, HazelcastConventions.PHYSICAL, distribution, collation);
    }

    /**
     * Convert the given input into physical input.
     *
     * @param input Original input.
     * @return Logical input.
     */
    public static RelNode toPhysicalInput(RelNode input) {
        return convert(input, toPhysicalConvention(input.getTraitSet()));
    }

    /**
     * Convert the given input into physical input.
     *
     * @param input Original input.
     * @param distribution Distribution.
     * @return Logical input.
     */
    public static RelNode toPhysicalInput(RelNode input, DistributionTrait distribution) {
        return convert(input, toPhysicalConvention(input.getTraitSet(), distribution));
    }

    /**
     * @param rel Node.
     * @return {@code true} if the given node is physical node.
     */
    public static boolean isPhysical(RelNode rel) {
        return rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE).equals(HazelcastConventions.PHYSICAL);
    }

    /**
     * Get possible physical rels from the given subset. Every returned input is guaranteed to have a unique trait set.
     *
     * @param subset Subset.
     * @return Physical rels.
     */
    public static Collection<RelNode> getPhysicalRelsFromSubset(RelNode subset) {
        if (subset instanceof RelSubset) {
            RelSubset subset0 = (RelSubset) subset;

            Set<RelTraitSet> traitSets = new HashSet<>();

            Set<RelNode> res = Collections.newSetFromMap(new IdentityHashMap<>());

            for (RelNode rel : subset0.getRelList()) {
                if (!isPhysical(rel)) {
                    continue;
                }

                if (traitSets.add(rel.getTraitSet())) {
                    res.add(convert(subset, rel.getTraitSet()));
                }
            }

            return res;
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * Get combinations of trait sets of physical relations of the given subset.
     *
     * @param subset Subset.
     * @return Trait sets.
     */
    public static Set<RelTraitSet> getPhysicalTraitSets(RelSubset subset) {
        Set<RelTraitSet> traitSets = new HashSet<>();

        for (RelNode rel : subset.getRelList()) {
            if (!isPhysical(rel)) {
                continue;
            }

            traitSets.add(rel.getTraitSet());
        }

        return traitSets;
    }

    /**
     * Get collation of the given node.
     *
     * @param rel Rel node.
     * @return Physical distribution.
     */
    public static RelCollation getCollation(RelNode rel) {
        return rel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
    }

    /**
     * Convenient method to get cost of a relation.
     *
     * @param rel Relation.
     * @return Cost.
     */
    public static RelOptCost getCost(RelNode rel) {
        RelOptCluster cluster = rel.getCluster();

        return cluster.getPlanner().getCost(rel, cluster.getMetadataQuery());
    }

    public static boolean isHazelcastTable(TableScan scan) {
        HazelcastTable table = scan.getTable().unwrap(HazelcastTable.class);

        return table != null;
    }

    public static HazelcastTable getHazelcastTable(TableScan scan) {
        HazelcastTable table = scan.getTable().unwrap(HazelcastTable.class);

        assert table != null;

        return table;
    }

    public static HazelcastRelOptCluster getCluster(RelNode rel) {
        assert rel.getCluster() instanceof HazelcastRelOptCluster;

        return (HazelcastRelOptCluster) rel.getCluster();
    }

    public static DistributionTraitDef getDistributionDef(RelNode rel) {
        return getCluster(rel).getDistributionTraitDef();
    }

    public static DistributionTrait getDistribution(RelNode rel) {
        return rel.getTraitSet().getTrait(getDistributionDef(rel));
    }

    public static HazelcastRelOptTable createRelTable(
        HazelcastRelOptTable originalRelTable,
        HazelcastTable newHazelcastTable,
        RelDataTypeFactory typeFactory
    ) {
        RelOptTableImpl newTable = RelOptTableImpl.create(
            originalRelTable.getRelOptSchema(),
            newHazelcastTable.getRowType(typeFactory),
            originalRelTable.getDelegate().getQualifiedName(),
            newHazelcastTable,
            null
        );

        return new HazelcastRelOptTable(newTable);
    }

    public static LogicalTableScan createLogicalScanWithNewTable(
        TableScan originalScan,
        HazelcastTable newHazelcastTable
    ) {
        HazelcastRelOptTable originalRelTable = (HazelcastRelOptTable) originalScan.getTable();

        HazelcastRelOptTable newTable = createRelTable(
            originalRelTable,
            newHazelcastTable,
            originalScan.getCluster().getTypeFactory()
        );

        return LogicalTableScan.create(
            originalScan.getCluster(),
            newTable,
            originalScan.getHints()
        );
    }
}
