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
package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.catalog.{GenericInMemoryCatalog, ObjectPath}
import org.apache.flink.table.catalog.stats.CatalogTableStatistics
import org.apache.flink.table.planner.plan.optimize.program.{FlinkBatchProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.utils.{TableConfigUtils, TableTestBase}

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules.CoreRules
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

/** Tests for [[org.apache.flink.table.planner.plan.rules.logical.FlinkJoinToMultiJoinRule]]. */
class FlinkJoinToMultiJoinRuleTest extends TableTestBase {

  private val util = batchTestUtil()
  private val catalog = new GenericInMemoryCatalog("catalog1", "default_db")

  @Before
  def setup(): Unit = {
    util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE)
    val calciteConfig = TableConfigUtils.getCalciteConfig(util.tableEnv.getConfig)
    calciteConfig.getBatchProgram.get.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(FlinkJoinToMultiJoinRule.INSTANCE, CoreRules.PROJECT_MULTI_JOIN_MERGE))
        .build()
    )

    catalog.open()
    util.tableEnv.registerCatalog("catalog1", catalog)
    util.tableEnv.useCatalog("catalog1")

    val ddl1 =
      """
        | CREATE TABLE catalog1.default_db.T1 ( 
        |  a int,
        |  b bigint
        |  ) WITH (
        |  'connector' = 'values',
        |  'bounded' = 'true'
        |  )
        |""".stripMargin
    util.tableEnv.executeSql(ddl1)

    val ddl2 =
      """
        | CREATE TABLE catalog1.default_db.T2 ( 
        |  c int,
        |  d bigint
        |  ) WITH (
        |  'connector' = 'values',
        |  'bounded' = 'true'
        |  )
        |""".stripMargin
    util.tableEnv.executeSql(ddl2)

    val ddl3 =
      """
        | CREATE TABLE catalog1.default_db.T3 ( 
        |  e int,
        |  f bigint
        |  ) WITH (
        |  'connector' = 'values',
        |  'bounded' = 'true'
        |  )
        |""".stripMargin
    util.tableEnv.executeSql(ddl3)

    val ddl4 =
      """
        | CREATE TABLE catalog1.default_db.T4 ( 
        |  g int,
        |  h bigint
        |  ) WITH (
        |  'connector' = 'values',
        |  'bounded' = 'true'
        |  )
        |""".stripMargin
    util.tableEnv.executeSql(ddl4)

    val ddl5 =
      """
        | CREATE TABLE catalog1.default_db.T5 ( 
        |  i int,
        |  j bigint
        |  ) WITH (
        |  'connector' = 'values',
        |  'bounded' = 'true'
        |  )
        |""".stripMargin
    util.tableEnv.executeSql(ddl5)
  }

  @Test
  def testInnerJoinInnerJoin(): Unit = {
    // Can not translate join to multi join because these tables have no table statistics.
    val sqlQuery = "SELECT * FROM T1, T2, T3 WHERE a = c AND a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInnerJoinInnerJoinWithStatistics(): Unit = {
    // Can translate join to multi join
    alterTableStatics()
    val sqlQuery = "SELECT * FROM T1, T2, T3 WHERE a = c AND a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInnerJoinLeftOuterJoin(): Unit = {
    val sqlQuery = "SELECT * FROM T1 JOIN T2 ON a =c LEFT OUTER JOIN T3 ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInnerJoinLeftOuterJoinWithStatistics(): Unit = {
    alterTableStatics()
    val sqlQuery = "SELECT * FROM T1 JOIN T2 ON a =c LEFT OUTER JOIN T3 ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInnerJoinRightOuterJoin(): Unit = {
    // Cannot translate to one multi join set because right outer join left will generate null.
    val sqlQuery = "SELECT * FROM T1 JOIN T2 ON a =c RIGHT OUTER JOIN T3 ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinLeftOuterJoin(): Unit = {
    // Can not translate join to multi join because of no table statistics.
    val sqlQuery =
      "SELECT * FROM T1 LEFT OUTER JOIN T2 ON a = c LEFT OUTER JOIN (SELECT * FROM T3) ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinLeftOuterJoinWithStatistics(): Unit = {
    // Can translate join to multi join.
    alterTableStatics()
    val sqlQuery =
      "SELECT * FROM T1 LEFT OUTER JOIN T2 ON a = c LEFT OUTER JOIN (SELECT * FROM T3) ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinRightOuterJoin(): Unit = {
    // Cannot translate join to multi join.
    val sqlQuery =
      "SELECT * FROM T1 LEFT OUTER JOIN T2 ON a = c RIGHT OUTER JOIN (SELECT * FROM T3) ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinInnerJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 LEFT OUTER JOIN T2 ON a = c JOIN (SELECT * FROM T3) ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinInnerJoinWithStatistics(): Unit = {
    alterTableStatics()
    val sqlQuery =
      "SELECT * FROM T1 LEFT OUTER JOIN T2 ON a = c JOIN (SELECT * FROM T3) ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testRightOuterJoinRightOuterJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c RIGHT OUTER JOIN (SELECT * FROM T3) ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testSubRightOuterJoinQuery(): Unit = {
    val sqlQuery =
      "SELECT * FROM T3 RIGHT OUTER JOIN (SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c) t ON t.a = T3.e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testSubRightOuterJoinQueryWithStatistics(): Unit = {
    // This case will be set into one multi join set.
    alterTableStatics()
    val sqlQuery =
      "SELECT * FROM T3 RIGHT OUTER JOIN (SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c) t ON t.a = T3.e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testRightOuterJoinLeftOuterJoin(): Unit = {
    // Cannot not translate join to multi join because right outer join in join left.
    val sqlQuery =
      "SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c LEFT OUTER JOIN (SELECT * FROM T3) ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testRightOuterJoinInnerJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c JOIN (SELECT * FROM T3) ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testRightOuterJoinInnerJoinWithStatistics(): Unit = {
    alterTableStatics()
    val sqlQuery =
      "SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c JOIN (SELECT * FROM T3) ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testFullOuterJoin(): Unit = {
    // Cannot translate join to multi join.
    val sqlQuery = "SELECT * FROM T1 FULL OUTER JOIN T2 ON a = c, T3 WHERE a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testFullOuterJoinInnerJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 FULL OUTER JOIN T2 ON a = c JOIN (SELECT * FROM T3) ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testFullOuterJoinLeftOuterJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 FULL OUTER JOIN T2 ON a = c LEFT OUTER JOIN (SELECT * FROM T3) ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testFullOuterJoinRightOuterJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 FULL OUTER JOIN T2 ON a = c RIGHT OUTER JOIN (SELECT * FROM T3) ON a = e"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testFullOuterJoinSemiJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 FULL OUTER JOIN T2 ON a = c WHERE a IN (SELECT e FROM T3)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInnerJoinSemiJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM (SELECT * FROM T1 JOIN T2 ON a = c) t WHERE a IN (SELECT e FROM T3)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinSemiJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 LEFT OUTER JOIN T2 ON a = c WHERE a IN (SELECT e FROM T3)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testRightOuterJoinSemiJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c WHERE a IN (SELECT e FROM T3)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInnerJoinAntiJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (SELECT * FROM T1 JOIN T2 ON a = c) t
        |WHERE NOT EXISTS (SELECT e FROM T3  WHERE a = e)
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinAntiJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (SELECT * FROM T1 LEFT OUTER JOIN T2 ON a = c) t
        |WHERE NOT EXISTS (SELECT e FROM T3  WHERE a = e)
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testRightOuterJoinAntiJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c) t
        |WHERE NOT EXISTS (SELECT e FROM T3  WHERE a = e)
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInnerJoinLeftOuterJoinInnerJoinLeftOuterJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM T1 JOIN T2 ON a = c LEFT OUTER JOIN 
        |(SELECT * FROM T3) ON a = e JOIN
        |(SELECT * FROM T4) ON a = g LEFT OUTER JOIN
        |(SELECT * FROM T5) ON a = i
        """.stripMargin
    util.verifyRelPlan(sqlQuery)

  }

  @Test
  def testInnerJoinLeftOuterJoinInnerJoinLeftOuterJoinWithStatistics(): Unit = {
    alterTableStatics()
    val sqlQuery =
      """
        |SELECT * FROM T1 JOIN T2 ON a = c LEFT OUTER JOIN 
        |(SELECT * FROM T3) ON a = e JOIN
        |(SELECT * FROM T4) ON a = g LEFT OUTER JOIN
        |(SELECT * FROM T5) ON a = i
        """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinInnerJoinLeftOuterJoinInnerJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM T1 LEFT OUTER JOIN T2 ON a = c JOIN 
        |(SELECT * FROM T3) ON a = e LEFT OUTER JOIN
        |(SELECT * FROM T4) ON a = g JOIN
        |(SELECT * FROM T5) ON a = i
        """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinInnerJoinLeftOuterJoinInnerJoinWithStatistics(): Unit = {
    alterTableStatics()
    val sqlQuery =
      """
        |SELECT * FROM T1 LEFT OUTER JOIN T2 ON a = c JOIN 
        |(SELECT * FROM T3) ON a = e LEFT OUTER JOIN
        |(SELECT * FROM T4) ON a = g JOIN
        |(SELECT * FROM T5) ON a = i
        """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInnerJoinRightOuterJoinInnerJoinRightOuterJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM T1 JOIN T2 ON a = c RIGHT OUTER JOIN 
        |(SELECT * FROM T3) ON a = e JOIN
        |(SELECT * FROM T4) ON a = g RIGHT OUTER JOIN
        |(SELECT * FROM T5) ON a = i
        """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInnerJoinRightOuterJoinInnerJoinRightOuterJoinWithStatistics(): Unit = {
    alterTableStatics()
    val sqlQuery =
      """
        |SELECT * FROM T1 JOIN T2 ON a = c RIGHT OUTER JOIN 
        |(SELECT * FROM T3) ON a = e JOIN
        |(SELECT * FROM T4) ON a = g RIGHT OUTER JOIN
        |(SELECT * FROM T5) ON a = i
        """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testRightOuterJoinInnerJoinRightOuterJoinInnerJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c JOIN
        |(SELECT * FROM T3) ON a = e RIGHT OUTER JOIN
        |(SELECT * FROM T4) ON a = g JOIN
        |(SELECT * FROM T5) ON a = i
        """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  def alterTableStatics(): Unit = {
    catalog.alterTableStatistics(
      new ObjectPath("default_db", "T1"),
      new CatalogTableStatistics(100, 10, 10, 10),
      false)
    catalog.alterTableStatistics(
      new ObjectPath("default_db", "T2"),
      new CatalogTableStatistics(1000, 10, 10, 10),
      false)
    catalog.alterTableStatistics(
      new ObjectPath("default_db", "T3"),
      new CatalogTableStatistics(10000, 10, 10, 10),
      false)
    catalog.alterTableStatistics(
      new ObjectPath("default_db", "T4"),
      new CatalogTableStatistics(10000, 100, 100, 100),
      false)
    catalog.alterTableStatistics(
      new ObjectPath("default_db", "T5"),
      new CatalogTableStatistics(10000, 1000, 1000, 1000),
      false)
  }
}
