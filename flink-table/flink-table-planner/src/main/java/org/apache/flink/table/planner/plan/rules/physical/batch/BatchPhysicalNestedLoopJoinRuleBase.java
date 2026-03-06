/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.physical.batch;

import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalNestedLoopJoin;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;

/**
 * Base interface for nested loop join rules that provides common functionality for creating {@link
 * BatchPhysicalNestedLoopJoin}.
 */
public interface BatchPhysicalNestedLoopJoinRuleBase {

    /**
     * Creates a {@link BatchPhysicalNestedLoopJoin} with the appropriate trait sets based on the
     * join type and build side.
     *
     * @param join the original join node
     * @param left the left input
     * @param right the right input
     * @param leftIsBuild true if left side is the build side
     * @param singleRowJoin true if one side returns single row
     * @return the created BatchPhysicalNestedLoopJoin
     */
    default RelNode createNestedLoopJoin(
            Join join, RelNode left, RelNode right, boolean leftIsBuild, boolean singleRowJoin) {
        RelTraitSet leftRequiredTrait =
                join.getTraitSet().replace(FlinkConventions.BATCH_PHYSICAL());
        RelTraitSet rightRequiredTrait =
                join.getTraitSet().replace(FlinkConventions.BATCH_PHYSICAL());

        if (join.getJoinType() == JoinRelType.FULL) {
            leftRequiredTrait = leftRequiredTrait.replace(FlinkRelDistribution.SINGLETON());
            rightRequiredTrait = rightRequiredTrait.replace(FlinkRelDistribution.SINGLETON());
        } else {
            if (leftIsBuild) {
                leftRequiredTrait =
                        leftRequiredTrait.replace(FlinkRelDistribution.BROADCAST_DISTRIBUTED());
            } else {
                rightRequiredTrait =
                        rightRequiredTrait.replace(FlinkRelDistribution.BROADCAST_DISTRIBUTED());
            }
        }

        RelNode newLeft = RelOptRule.convert(left, leftRequiredTrait);
        RelNode newRight = RelOptRule.convert(right, rightRequiredTrait);
        RelTraitSet providedTraitSet =
                join.getTraitSet().replace(FlinkConventions.BATCH_PHYSICAL());

        return new BatchPhysicalNestedLoopJoin(
                join.getCluster(),
                providedTraitSet,
                newLeft,
                newRight,
                join.getCondition(),
                join.getJoinType(),
                leftIsBuild,
                singleRowJoin);
    }
}
