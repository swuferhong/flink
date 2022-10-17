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
package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.table.api._
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.catalog.ObjectPath
import org.apache.flink.table.catalog.stats.CatalogTableStatistics
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Before, Test}

/** Test for [[RemoveRedundantLocalSortAggRule]]. */
class RemoveRedundantLocalSortAggRuleTest extends TableTestBase {

  private val util = batchTestUtil()
  private val tEnv: TableEnvironment = util.tableEnv

  @Before
  def setup(): Unit = {
    val ddl1 =
      """
        |CREATE TABLE x (a int, b bigint, c string) WITH (
        | 'connector' = 'values',
        | 'bounded' = 'true'
        | )
        |""".stripMargin
    tEnv.executeSql(ddl1)
    tEnv
      .getCatalog("default_catalog")
      .get()
      .alterTableStatistics(
        new ObjectPath("default_database", "x"),
        new CatalogTableStatistics(100000001L, 10, 10L, 10L),
        false)

    val ddl2 =
      """
        |CREATE TABLE y (d int, e bigint, f string) WITH (
        | 'connector' = 'values',
        | 'bounded' = 'true'
        | )
        |""".stripMargin
    tEnv.executeSql(ddl2)
    tEnv
      .getCatalog("default_catalog")
      .get()
      .alterTableStatistics(
        new ObjectPath("default_database", "y"),
        new CatalogTableStatistics(100000001L, 10, 10L, 10L),
        false)

    val ddl3 =
      """
        |CREATE TABLE z (a int, b bigint, c bigint, d bigint, e bigint) WITH (
        | 'connector' = 'values',
        | 'bounded' = 'true'
        | )
        |""".stripMargin
    tEnv.executeSql(ddl3)
    tEnv
      .getCatalog("default_catalog")
      .get()
      .alterTableStatistics(
        new ObjectPath("default_database", "z"),
        new CatalogTableStatistics(100000001L, 10, 10L, 10L),
        false)
  }

  @Test
  def testRemoveRedundantLocalSortAggWithSort(): Unit = {
    tEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
      "SortMergeJoin,NestedLoopJoin,HashAgg")
    // disable BroadcastHashJoin
    tEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, Long.box(-1))
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT sum(b) FROM r group by a
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testRemoveRedundantLocalSortAggWithoutSort(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin,HashAgg")
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT sum(b) FROM r group by a
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testUsingLocalAggCallFilters(): Unit = {
    tEnv.getConfig.set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashAgg")
    val sqlQuery = "SELECT d, MAX(e), MAX(e) FILTER (WHERE a < 10), COUNT(DISTINCT c),\n" +
      "COUNT(DISTINCT c) FILTER (WHERE a > 5), COUNT(DISTINCT b) FILTER (WHERE b > 3)\n" +
      "FROM z GROUP BY d"
    util.verifyRelPlan(sqlQuery)
  }

}
