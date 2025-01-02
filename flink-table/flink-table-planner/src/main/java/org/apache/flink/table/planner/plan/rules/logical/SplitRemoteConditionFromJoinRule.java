/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Rule will split the {@link FlinkLogicalJoin} which contains Remote Functions in join condition
 * into a {@link FlinkLogicalJoin} and a {@link FlinkLogicalCalc} with Remote Functions. Currently,
 * only inner join is supported.
 *
 * <p>After this rule is applied, there will be no Remote Functions in the condition of the {@link
 * FlinkLogicalJoin}.
 */
public final class SplitRemoteConditionFromJoinRule extends RelOptRule {
    protected final RemoteCalcCallFinder callFinder;
    protected final Optional<String> errorOnUnsplittableRemoteCall;

    public SplitRemoteConditionFromJoinRule(
            RemoteCalcCallFinder callFinder, Optional<String> errorOnUnsplittableRemoteCall) {
        super(operand(FlinkLogicalJoin.class, none()), "SplitRemoteConditionFromJoinRule");
        this.callFinder = callFinder;
        this.errorOnUnsplittableRemoteCall = errorOnUnsplittableRemoteCall;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalJoin join = call.rel(0);
        JoinRelType joinType = join.getJoinType();
        // matches if it is inner join and it contains Remote functions in condition
        if (join.getCondition() != null && callFinder.containsRemoteCall(join.getCondition())) {
            if (joinType == JoinRelType.INNER) {
                return true;
            } else if (errorOnUnsplittableRemoteCall.isPresent()) {
                throw new TableException(errorOnUnsplittableRemoteCall.get());
            }
        }
        return false;
    }

    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalJoin join = call.rel(0);
        RexBuilder rexBuilder = join.getCluster().getRexBuilder();

        List<RexNode> joinFilters = RelOptUtil.conjunctions(join.getCondition());
        List<RexNode> remoteFilters =
                joinFilters.stream()
                        .filter(callFinder::containsRemoteCall)
                        .collect(Collectors.toList());
        List<RexNode> remainingFilters =
                joinFilters.stream()
                        .filter(f -> !callFinder.containsRemoteCall(f))
                        .collect(Collectors.toList());

        RexNode newJoinCondition = RexUtil.composeConjunction(rexBuilder, remainingFilters);
        FlinkLogicalJoin bottomJoin =
                new FlinkLogicalJoin(
                        join.getCluster(),
                        join.getTraitSet(),
                        join.getLeft(),
                        join.getRight(),
                        newJoinCondition,
                        join.getHints(),
                        join.getJoinType());

        RexProgramBuilder rexProgramBuilder =
                new RexProgramBuilder(bottomJoin.getRowType(), rexBuilder);
        RexProgram rexProgram = rexProgramBuilder.getProgram();
        RexNode topCalcCondition = RexUtil.composeConjunction(rexBuilder, remoteFilters);

        FlinkLogicalCalc topCalc =
                new FlinkLogicalCalc(
                        join.getCluster(),
                        join.getTraitSet(),
                        bottomJoin,
                        RexProgram.create(
                                bottomJoin.getRowType(),
                                rexProgram.getExprList(),
                                topCalcCondition,
                                bottomJoin.getRowType(),
                                rexBuilder));

        call.transformTo(topCalc);
    }

    // Consider the rules to be equal if they are the same class and their call finders are the same
    // class.
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof SplitRemoteConditionFromJoinRule)) {
            return false;
        }

        SplitRemoteConditionFromJoinRule rule = (SplitRemoteConditionFromJoinRule) obj;
        return super.equals(rule)
                && callFinder.getClass().equals(rule.callFinder.getClass())
                && errorOnUnsplittableRemoteCall.equals(rule.errorOnUnsplittableRemoteCall);
    }
}
