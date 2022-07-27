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

import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalDynamicFilteringTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalJoinBase;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalTableSourceScan;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Join;

import java.util.Arrays;

/** DynamicFilteringRule1. */
public class DynamicFilteringRule5 extends DynamicFilteringRuleBase {

    public static final RelOptRule FACT_IN_RIGHT =
            DynamicFilteringRule5.Config.EMPTY
                    .withDescription("DynamicFilteringRule5:factInRight")
                    .as(Config.class)
                    .factInRight()
                    .toRule();

    public DynamicFilteringRule5(RelRule.Config config) {
        super(config);
    }

    /** Config. */
    public interface Config extends RelRule.Config {
        @Override
        default DynamicFilteringRule5 toRule() {
            return new DynamicFilteringRule5(this);
        }

        default Config factInRight() {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(BatchPhysicalJoinBase.class)
                                            .inputs(
                                                    l ->
                                                            l.operand(BatchPhysicalRel.class)
                                                                    .anyInputs(),
                                                    r ->
                                                            r.operand(BatchPhysicalCalc.class)
                                                                    .oneInput(
                                                                            f ->
                                                                                    f.operand(
                                                                                                    BatchPhysicalTableSourceScan
                                                                                                            .class)
                                                                                            .noInputs())))
                    .as(Config.class);
        }
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final BatchPhysicalJoinBase join = call.rel(0);
        final BatchPhysicalRel dimSide = call.rel(1);
        final BatchPhysicalTableSourceScan factScan = call.rel(3);
        return doMatches(join, dimSide, factScan, false);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final BatchPhysicalJoinBase join = call.rel(0);
        final BatchPhysicalRel dimSide = call.rel(1);
        final BatchPhysicalTableSourceScan factScan = call.rel(3);

        final BatchPhysicalDynamicFilteringTableSourceScan newFactScan =
                createDynamicFilteringTableSourceScan(factScan, dimSide, join, false);
        final Join newJoin = join.copy(join.getTraitSet(), Arrays.asList(dimSide, newFactScan));
        call.transformTo(newJoin);
    }
}
