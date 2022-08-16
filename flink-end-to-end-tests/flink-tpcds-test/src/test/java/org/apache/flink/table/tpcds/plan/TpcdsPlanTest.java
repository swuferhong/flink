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

package org.apache.flink.table.tpcds.plan;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TpcdsPlanTest extends TableTestBase {
    protected final BatchTableTestUtil util = batchTestUtil(new TableConfig());

    @Parameterized.Parameter public String caseName;
    protected TableEnvironment tEnv = util.getTableEnv();

    @Before
    public void before() throws Exception {
        // config Optimizer parameters
        tEnv.getConfig()
                .getConfiguration()
                .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
        tEnv.getConfig()
                .getConfiguration()
                .setLong(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD,
                        10 * 1024 * 1024);
        tEnv.getConfig()
                .getConfiguration()
                .setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true);
        tEnv.getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE, "ALL_EDGES_PIPELINED");
        tEnv.getConfig()
                .getConfiguration()
                .setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_ENABLED, true);
        tEnv.getConfig()
                .getConfiguration()
                .setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, false);
        tEnv.getConfig()
                .getConfiguration()
                .setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_DYNAMIC_FILTERING_ENABLED, true);
        tEnv.getConfig()
                .getConfiguration()
                .set(ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_BLOCKING);
    }

    @Test
    public void testPlan() throws Exception {
        String sql = getSqlFile(caseName);
        util.verifyExecPlan(sql);
    }

    @Parameterized.Parameters(name = "q{0}")
    public static Collection<String> parameters() {
        return Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10");
    }

    protected String getSqlFile(String caseName) {
        return TableTestUtil.readFromResource("/tpcds/q" + caseName + ".sql");
    }
}
