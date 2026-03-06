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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.hint.JoinStrategy;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalNestedLoopJoin;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Rule that converts {@link FlinkLogicalJoin} to {@link BatchPhysicalNestedLoopJoin} if
 * NestedLoopJoin is enabled.
 */
@Value.Enclosing
public class BatchPhysicalNestedLoopJoinRule
        extends RelRule<BatchPhysicalNestedLoopJoinRule.BatchPhysicalNestedLoopJoinRuleConfig>
        implements BatchPhysicalJoinRuleBase, BatchPhysicalNestedLoopJoinRuleBase {

    public static final BatchPhysicalNestedLoopJoinRule INSTANCE =
            BatchPhysicalNestedLoopJoinRuleConfig.DEFAULT.toRule();

    protected BatchPhysicalNestedLoopJoinRule(BatchPhysicalNestedLoopJoinRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Join join = call.rel(0);
        final TableConfig tableConfig = ShortcutUtils.unwrapTableConfig(join);
        return canUseJoinStrategy(join, tableConfig, JoinStrategy.NEST_LOOP);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Join join = call.rel(0);
        final TableConfig tableConfig = ShortcutUtils.unwrapTableConfig(join);

        final RelNode left = join.getLeft();
        RelNode right;

        if (join.getJoinType() == JoinRelType.SEMI || join.getJoinType() == JoinRelType.ANTI) {
            // We can do a distinct to buildSide(right) when semi join.
            List<Integer> distinctKeys = new ArrayList<>();
            for (int i = 0; i < join.getRight().getRowType().getFieldCount(); i++) {
                distinctKeys.add(i);
            }
            Seq<Integer> distinctKeysSeq =
                    JavaConverters.asScalaBufferConverter(distinctKeys).asScala();
            boolean useBuildDistinct = chooseSemiBuildDistinct(join.getRight(), distinctKeysSeq);
            if (useBuildDistinct) {
                right = addLocalDistinctAgg(join.getRight(), distinctKeysSeq);
            } else {
                right = join.getRight();
            }
        } else {
            right = join.getRight();
        }

        final scala.Option<JoinStrategy> firstValidJoinHintOpt =
                getFirstValidJoinHint(join, tableConfig);

        final Join temJoin =
                join.copy(
                        join.getTraitSet(),
                        join.getCondition(),
                        left,
                        right,
                        join.getJoinType(),
                        join.isSemiJoinDone());

        final boolean isLeftToBuild;
        if (firstValidJoinHintOpt.isDefined()) {
            JoinStrategy firstValidJoinHint = firstValidJoinHintOpt.get();
            if (firstValidJoinHint == JoinStrategy.NEST_LOOP) {
                Tuple2<Boolean, Boolean> result = checkNestLoopJoin(temJoin, tableConfig, true);
                isLeftToBuild = result._2();
            } else {
                // this should not happen
                throw new TableException(
                        String.format(
                                "The planner is trying to convert the "
                                        + "`FlinkLogicalJoin` using NEST_LOOP, but the valid join hint is not NEST_LOOP: %s",
                                firstValidJoinHint));
            }
        } else {
            // treat as non-join-hints
            Tuple2<Boolean, Boolean> result = checkNestLoopJoin(temJoin, tableConfig, false);
            isLeftToBuild = result._2();
        }

        final RelNode newJoin = createNestedLoopJoin(join, left, right, isLeftToBuild, false);
        call.transformTo(newJoin);
    }

    /** Configuration for {@link BatchPhysicalNestedLoopJoinRule}. */
    @Value.Immutable(singleton = false)
    public interface BatchPhysicalNestedLoopJoinRuleConfig extends RelRule.Config {

        BatchPhysicalNestedLoopJoinRuleConfig DEFAULT =
                ImmutableBatchPhysicalNestedLoopJoinRule.BatchPhysicalNestedLoopJoinRuleConfig
                        .builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(FlinkLogicalJoin.class)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(RelNode.class)
                                                                        .anyInputs()))
                        .withDescription("BatchPhysicalNestedLoopJoinRule")
                        .as(BatchPhysicalNestedLoopJoinRuleConfig.class);

        @Override
        default BatchPhysicalNestedLoopJoinRule toRule() {
            return new BatchPhysicalNestedLoopJoinRule(this);
        }
    }
}
