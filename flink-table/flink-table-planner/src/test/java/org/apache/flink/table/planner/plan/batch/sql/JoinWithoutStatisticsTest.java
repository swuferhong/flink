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

package org.apache.flink.table.planner.plan.batch.sql;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.planner.plan.batch.sql.join.JoinTestBase;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class JoinWithoutStatisticsTest extends JoinTestBase {

    @Before
    @Override
    public void before() {
        super.before();
        try {
            catalog()
                    .alterTableStatistics(
                            new ObjectPath("default_db", "MyTable1"),
                            CatalogTableStatistics.UNKNOWN,
                            false);
            catalog()
                    .alterTableStatistics(
                            new ObjectPath("default_db", "MyTable2"),
                            CatalogTableStatistics.UNKNOWN,
                            false);
            ObjectPath tablePath = new ObjectPath("default_db", "partition_t");
            catalog()
                    .alterPartitionStatistics(
                            tablePath,
                            new CatalogPartitionSpec(Collections.singletonMap("k", "1990")),
                            CatalogTableStatistics.UNKNOWN,
                            false);
            catalog()
                    .alterPartitionStatistics(
                            tablePath,
                            new CatalogPartitionSpec(Collections.singletonMap("k", "1991")),
                            CatalogTableStatistics.UNKNOWN,
                            false);
        } catch (PartitionNotExistException | TableNotExistException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testInnerJoinWithoutStatistics() {
        // BroadCast join and hash join are disable while table have no statistics.
        util().verifyExecPlan("SELECT c, g FROM MyTable2 INNER JOIN MyTable1 ON a = d AND d < 2");
    }

    @Test
    public void testInnerJoinWithoutStatisticsDisableJoinType() {
        util().getTableEnv()
                .getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "NestedLoopJoin,SortMergeJoin");
        thrown().expect(TableException.class);
        thrown().expectMessage(
                        "No join operator can be selected, please add statistics for "
                                + "source or don't disable SortMergeJoin and NestedLoopJoin");
        util().verifyExecPlan("SELECT c, g FROM MyTable2 INNER JOIN MyTable1 ON a = d AND d < 2");
    }

    @Test
    public void testInnerJoinWithOneTableHaveStatisticsAndBuildInLeft()
            throws TableNotExistException {
        catalog()
                .alterTableStatistics(
                        new ObjectPath("default_db", "MyTable2"),
                        new CatalogTableStatistics(100L, 10, 100L, 10L),
                        false);
        util().verifyExecPlan("SELECT c, g FROM MyTable2 INNER JOIN MyTable1 ON a = d AND d < 2");
    }

    @Test
    public void testInnerJoinWithOneTableHaveStatisticsAndBuildInRight()
            throws TableNotExistException {
        catalog()
                .alterTableStatistics(
                        new ObjectPath("default_db", "MyTable1"),
                        new CatalogTableStatistics(100L, 10, 100L, 10L),
                        false);
        util().verifyExecPlan("SELECT c, g FROM MyTable2 INNER JOIN MyTable1 ON a = d AND d < 2");
    }

    @Test
    public void testInnerJoinWithPartialTablesHaveStatistics() throws TableNotExistException {
        String ddl1 =
                "CREATE TABLE MyTable3 (i int, j bigint) WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true')";
        tEnv().executeSql(ddl1);
        String ddl2 =
                "CREATE TABLE MyTable4 (k int, l bigint) WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true')";
        tEnv().executeSql(ddl2);
        catalog()
                .alterTableStatistics(
                        new ObjectPath("default_db", "MyTable3"),
                        new CatalogTableStatistics(100L, 100, 10L, 10L),
                        false);
        catalog()
                .alterTableStatistics(
                        new ObjectPath("default_db", "MyTable4"),
                        new CatalogTableStatistics(1000L, 100, 10L, 10L),
                        false);

        util().verifyExecPlan(
                        "SELECT c, g, i FROM MyTable1 INNER JOIN"
                                + " (SELECT * FROM MyTable2 WHERE d > 0) T2 ON a = d INNER JOIN "
                                + " (SELECT * FROM MyTable3 WHERE i > 10) T3 ON a = i INNER JOIN "
                                + " (SELECT * FROM MyTable4) T4 ON a = k");
    }

    @Test
    public void testInnerJoinWithAllTablesHaveNoStatistics() {
        String ddl1 =
                "CREATE TABLE MyTable3 (i int, j bigint) WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true')";
        tEnv().executeSql(ddl1);
        String ddl2 =
                "CREATE TABLE MyTable4 (k int, l bigint) WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true')";
        tEnv().executeSql(ddl2);

        util().verifyExecPlan(
                        "SELECT c, g, i FROM MyTable1 INNER JOIN"
                                + " (SELECT * FROM MyTable2 WHERE d > 0) T2 ON a = d INNER JOIN "
                                + " (SELECT * FROM MyTable3 WHERE i > 10) T3 ON a = i INNER JOIN "
                                + " (SELECT * FROM MyTable4) T4 ON a = k");
    }
}
