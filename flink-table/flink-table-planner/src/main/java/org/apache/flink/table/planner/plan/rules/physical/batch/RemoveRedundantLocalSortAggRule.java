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

import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWindowAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalLocalSortAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSortAggregate;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.immutables.value.Value;

import java.util.Optional;

/**
 * There maybe exist a subTree like localSortAggregate -> globalSortAggregate, or localSortAggregate
 * -> sort -> globalSortAggregate which the middle shuffle is removed. The rule could remove
 * redundant localSortAggregate node.
 */
public abstract class RemoveRedundantLocalSortAggRule extends RelOptRule {
    public RemoveRedundantLocalSortAggRule(RelOptRuleOperand operand, String ruleName) {
        super(operand, ruleName);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        BatchPhysicalSortAggregate globalAgg = getOriginalGlobalAgg(call);
        BatchPhysicalLocalSortAggregate localAgg = getOriginalLocalAgg(call);
        RelNode inputOfLocalAgg = getOriginalInputOfLocalAgg(call);
        BatchPhysicalSortAggregate newGlobalAgg = new BatchPhysicalSortAggregate(
                globalAgg.getCluster(),
                globalAgg.getTraitSet(),
                inputOfLocalAgg,
                globalAgg.getRowType(),
                inputOfLocalAgg.getRowType(),
                inputOfLocalAgg.getRowType(),
                localAgg.grouping(),
                localAgg.auxGrouping(),
                // Use the localAgg agg calls because the global agg call filters was removed,
                // see BatchPhysicalSortAggRule for details.
                localAgg.getAggCallToAggFunction(),
                false
        );
        call.transformTo(newGlobalAgg);
    }

    protected abstract BatchPhysicalSortAggregate getOriginalGlobalAgg(RelOptRuleCall call);

    protected abstract BatchPhysicalLocalSortAggregate getOriginalLocalAgg(RelOptRuleCall call);

    protected abstract RelNode getOriginalInputOfLocalAgg(RelOptRuleCall call);
}

public class RemoveRedundantLocalSortAggWithoutSortRule
        extends RemoveRedundantLocalSortAggRule<RemoveRedundantLocalSortAggWithoutSortRule.RemoveRedundantLocalSortAggWithoutSortRuleConfig> {

    protected RemoveRedundantLocalSortAggWithoutSortRule(RemoveRedundantLocalSortAggWithoutSortRuleConfig config) {
        super(config);
    }

    @Override
    protected BatchPhysicalSortAggregate getOriginalGlobalAgg(RelOptRuleCall call) {
        return null;
    }

    @Override
    protected BatchPhysicalLocalSortAggregate getOriginalLocalAgg(RelOptRuleCall call) {
        return null;
    }

    @Override
    protected RelNode getOriginalInputOfLocalAgg(RelOptRuleCall call) {
        return null;
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface RemoveRedundantLocalSortAggWithoutSortRuleConfig extends RelRule.Config {
        RemoveRedundantLocalSortAggWithoutSortRule.WindowPropertiesRule.RemoveRedundantLocalSortAggWithoutSortRuleConfig DEFAULT =
                ImmutableRemoveRedundantLocalSortAggWithoutSortRuleConfig.builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(LogicalProject.class)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(LogicalProject.class)
                                                                        .oneInput(
                                                                                b2 ->
                                                                                        b2.operand(
                                                                                                        LogicalWindowAggregate
                                                                                                                .class)
                                                                                                .noInputs())))
                        .withDescription("WindowPropertiesRule");

        @Override
        default RemoveRedundantLocalSortAggWithoutSortRule toRule() {
            return new RemoveRedundantLocalSortAggWithoutSortRule(this);
        }
    }
}



