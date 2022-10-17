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
package org.apache.flink.table.planner.plan.common

import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.catalog.{GenericInMemoryCatalog, ObjectPath}
import org.apache.flink.table.catalog.stats.{CatalogColumnStatistics, CatalogColumnStatisticsDataBase, CatalogColumnStatisticsDataLong, CatalogTableStatistics}
import org.apache.flink.table.planner.plan.rules.logical.JoinDeriveNullFilterRule
import org.apache.flink.table.planner.utils.{TableTestBase, TableTestUtil}

import org.junit.{Before, Test}

abstract class JoinReorderTestBase extends TableTestBase {

  protected val util: TableTestUtil = getTableTestUtil
  private val catalog = new GenericInMemoryCatalog("catalog1", "default_db")

  protected def getTableTestUtil: TableTestUtil

  protected def getProperties: String

  @Before
  def setup(): Unit = {

    catalog.open()
    util.tableEnv.registerCatalog("catalog1", catalog)
    util.tableEnv.useCatalog("catalog1")

    val ddl1 =
      """
        | CREATE TABLE catalog1.default_db.T1 ( 
        |  a1 int,
        |  b1 bigint,
        |  c1 string
        |  ) WITH (
        |  'connector' = 'values',
        |  %s
        |  )
        |""".stripMargin.format(getProperties)
    util.tableEnv.executeSql(ddl1)

    val ddl2 =
      """
        | CREATE TABLE catalog1.default_db.T2 ( 
        |  a2 int,
        |  b2 bigint,
        |  c2 string
        |  ) WITH (
        |  'connector' = 'values',
        |  %s
        |  )
        |""".stripMargin.format(getProperties)
    util.tableEnv.executeSql(ddl2)

    val ddl3 =
      """
        | CREATE TABLE catalog1.default_db.T3 ( 
        |  a3 int,
        |  b3 bigint,
        |  c3 string
        |  ) WITH (
        |  'connector' = 'values',
        |  %s
        |  )
        |""".stripMargin.format(getProperties)
    util.tableEnv.executeSql(ddl3)

    val ddl4 =
      """
        | CREATE TABLE catalog1.default_db.T4 ( 
        |  a4 int,
        |  b4 bigint,
        |  c4 string
        |  ) WITH (
        |  'connector' = 'values',
        |  %s
        |  )
        |""".stripMargin.format(getProperties)
    util.tableEnv.executeSql(ddl4)

    val ddl5 =
      """
        | CREATE TABLE catalog1.default_db.T5 (
        |  a5 int,
        |  b5 bigint,
        |  c5 string
        |  ) WITH (
        |  'connector' = 'values',
        |  %s
        |  )
        |""".stripMargin.format(getProperties)
    util.tableEnv.executeSql(ddl5)

    util.getTableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, Boolean.box(true))
  }

  @Test
  def testStarJoinCondition1(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4, T5
         |WHERE a1 = a2 AND a1 = a3 AND a1 = a4 AND a1 = a5
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testStarJoinConditionWithoutStatistics1(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4, T5
         |WHERE a1 = a2 AND a1 = a3 AND a1 = a4 AND a1 = a5
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testStarJoinCondition2(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4, T5
         |WHERE b1 = b2 AND b1 = b3 AND b1 = b4 AND b1 = b5
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testStarJoinConditionWithoutStatistics2(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4, T5
         |WHERE b1 = b2 AND b1 = b3 AND b1 = b4 AND b1 = b5
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testBushyJoinCondition1(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4, T5
         |WHERE a1 = a2 AND a2 = a3 AND a1 = a4 AND a3 = a5
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testBushyJoinCondition2(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4, T5
         |WHERE b1 = b2 AND b2 = b3 AND b1 = b4 AND b3 = b5
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testWithoutColumnStats(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4, T5
         |WHERE c1 = c2 AND c1 = c3 AND c2 = c4 AND c1 = c5
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinWithProject(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |WITH V1 AS (SELECT b1, a1, a2, c2 FROM T1 JOIN T2 ON a1 = a2),
         |     V2 AS (SELECT a3, b1, a1, c2, c3 FROM V1 JOIN T3 ON a2 = a3),
         |     V3 AS (SELECT a3, b1, a1, c2, c3, a4, b4 FROM T4 JOIN V2 ON a1 = a4)
         |
         |SELECT * FROM V3, T5 where a4 = a5
         """.stripMargin
    // can not reorder now
    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinWithFilter(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |WITH V1 AS (SELECT * FROM T1 JOIN T2 ON a1 = a2 WHERE b1 * b2 > 10),
         |     V2 AS (SELECT * FROM V1 JOIN T3 ON a2 = a3 WHERE b1 * b3 < 2000),
         |     V3 AS (SELECT * FROM T4 JOIN V2 ON a3 = a4 WHERE b2 + b4 > 100)
         |
         |SELECT * FROM V3, T5 WHERE a4 = a5 AND b5 < 15
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testInnerAndLeftOuterJoin(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |SELECT * FROM T1
         |   JOIN T2 ON a1 = a2
         |   JOIN T3 ON a2 = a3
         |   LEFT OUTER JOIN T4 ON a1 = a4
         |   JOIN T5 ON a4 = a5
         """.stripMargin
    // T1, T2, T3 T4 T5 can reorder.
    util.verifyExecPlan(sql)
  }

  @Test
  def testInnerAndRightOuterJoin(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |SELECT * FROM T1
         |   RIGHT OUTER JOIN T2 ON a1 = a2
         |   JOIN T3 ON a2 = a3
         |   JOIN T4 ON a1 = a4
         |   JOIN T5 ON a4 = a5
         """.stripMargin
    // T3, T4, T5 can reorder
    util.verifyExecPlan(sql)
  }

  @Test
  def testInnerAndFullOuterJoin(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |SELECT * FROM T1
         |   JOIN T2 ON a1 = a2
         |   FULL OUTER JOIN T3 ON a2 = a3
         |   JOIN T4 ON a1 = a4
         |   JOIN T5 ON a4 = a5
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testAllLeftOuterJoin(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |SELECT * FROM T1
         |   LEFT OUTER JOIN T2 ON a1 = a2
         |   LEFT OUTER JOIN T3 ON a2 = a3
         |   LEFT OUTER JOIN T4 ON a1 = a4
         |   LEFT OUTER JOIN T5 ON a4 = a5
         """.stripMargin
    // can reorder. Left outer join will be converted to one multi set by FlinkJoinToMultiJoinRule.
    util.verifyExecPlan(sql)
  }

  @Test
  def testAllRightOuterJoin(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |SELECT * FROM T1
         |   RIGHT OUTER JOIN T2 ON a1 = a2
         |   RIGHT OUTER JOIN T3 ON a2 = a3
         |   RIGHT OUTER JOIN T4 ON a1 = a4
         |   RIGHT OUTER JOIN T5 ON a4 = a5
         """.stripMargin
    // can not reorder
    util.verifyExecPlan(sql)
  }

  @Test
  def testAllFullOuterJoin(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |SELECT * FROM T1
         |   FULL OUTER JOIN T2 ON a1 = a2
         |   FULL OUTER JOIN T3 ON a1 = a3
         |   FULL OUTER JOIN T4 ON a1 = a4
         |   FULL OUTER JOIN T5 ON a4 = a5
         """.stripMargin
    // can not reorder
    util.verifyExecPlan(sql)
  }

  @Test
  def testInnerJoinLeftOuterJoinInnerJoinLeftOuterJoin(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |SELECT * FROM T1
         |   JOIN T2 ON a1 = a2
         |   LEFT OUTER JOIN T3 ON a1 = a3
         |   JOIN T4 ON a1 = a4
         |   LEFT OUTER JOIN T5 ON a4 = a5
         """.stripMargin
    // T1, T2, T3, T4, T5 can reorder.
    util.verifyExecPlan(sql)
  }

  @Test
  def testInnerJoinLeftOuterJoinInnerJoinLeftOuterJoinWithoutStatistics(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1
         |   JOIN T2 ON a1 = a2
         |   LEFT OUTER JOIN T3 ON a1 = a3
         |   JOIN T4 ON a1 = a4
         |   LEFT OUTER JOIN T5 ON a4 = a5
         """.stripMargin
    // Because these tables have no statistics, they can not reorder.
    util.verifyExecPlan(sql)
  }

  @Test
  def testLeftOuterJoinInnerJoinLeftOuterJoinInnerJoin(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |SELECT * FROM T1
         |   LEFT OUTER JOIN T2 ON a1 = a2
         |   JOIN T3 ON a1 = a3
         |   LEFT OUTER JOIN T4 ON a1 = a4
         |   JOIN T5 ON a4 = a5
         """.stripMargin
    // T1, T2, T3, T4, T5 can reorder.
    util.verifyExecPlan(sql)
  }

  @Test
  def testInnerJoinRightOuterJoinInnerJoinRightOuterJoin(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |SELECT * FROM T1
         |   JOIN T2 ON a1 = a2
         |   RIGHT OUTER JOIN T3 ON a1 = a3
         |   JOIN T4 ON a1 = a4
         |   RIGHT OUTER JOIN T5 ON a4 = a5
         """.stripMargin
    // T1 and T2 can not reorder, but MJ(T1, T2), T3, T4 can reorder.
    util.verifyExecPlan(sql)
  }

  @Test
  def testRightOuterJoinInnerJoinRightOuterJoinInnerJoin(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |SELECT * FROM T1
         |   RIGHT OUTER JOIN T2 ON a1 = a2
         |   JOIN T3 ON a1 = a3
         |   RIGHT OUTER JOIN T4 ON a1 = a4
         |   JOIN T5 ON a4 = a5
         """.stripMargin
    // T1, T2, T3 can reorder, and MJ(T1, T2, T3), T4, T5 can reorder.
    util.verifyExecPlan(sql)
  }

  @Test
  def testInnerJoinSemiJoin(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |SELECT * FROM T1
         |   JOIN T2 ON a1 = a2
         |   JOIN T3 ON a2 = a3
         |   JOIN T4 ON a1 = a4
         |   WHERE a1 IN (SELECT a5 FROM T5)
         """.stripMargin
    // can not reorder. Semi join will support join order in future.
    util.verifyExecPlan(sql)
  }

  @Test
  def testInnerJoinAntiJoin(): Unit = {
    alterTableStatistics()
    val sql =
      s"""
         |SELECT * FROM T1
         |   JOIN T2 ON a1 = a2
         |   JOIN T3 ON a2 = a3
         |   JOIN T4 ON a1 = a4
         |   WHERE NOT EXISTS (SELECT a5 FROM T5 WHERE a1 = a5)
         """.stripMargin
    // can not reorder
    util.verifyExecPlan(sql)
  }

  @Test
  def testDeriveNullFilterAfterJoinReorder(): Unit = {
    val tableStats = new CatalogTableStatistics(500000L, 100, 100, 100L)
    val columnAStats = new CatalogColumnStatisticsDataLong(null, null, 200000L, 50000L)
    val columnBStats = new CatalogColumnStatisticsDataLong(null, null, 100000L, 0L)
    var columnStatisticsData = new java.util.HashMap[String, CatalogColumnStatisticsDataBase]()

    val ddl1 =
      """
        | CREATE TABLE catalog1.default_db.T6 ( 
        |  a6 int,
        |  b6 bigint,
        |  c6 string
        |  ) WITH (
        |  'connector' = 'values',
        |  %s
        |  )
        |""".stripMargin.format(getProperties)
    util.tableEnv.executeSql(ddl1)
    catalog.alterTableStatistics(new ObjectPath("default_db", "T6"), tableStats, false)
    columnStatisticsData.put("a6", columnAStats)
    columnStatisticsData.put("b6", columnBStats)
    catalog.alterTableColumnStatistics(
      new ObjectPath("default_db", "T6"),
      new CatalogColumnStatistics(columnStatisticsData),
      false)

    val ddl2 =
      """
        | CREATE TABLE catalog1.default_db.T7 (
        |  a7 int,
        |  b7 bigint,
        |  c7 string
        |  ) WITH (
        |  'connector' = 'values',
        |  %s
        |  )
        |""".stripMargin.format(getProperties)
    util.tableEnv.executeSql(ddl2)
    columnStatisticsData = new java.util.HashMap[String, CatalogColumnStatisticsDataBase]()
    catalog.alterTableStatistics(new ObjectPath("default_db", "T7"), tableStats, false)
    columnStatisticsData.put("a7", columnAStats)
    columnStatisticsData.put("b7", columnBStats)
    catalog.alterTableColumnStatistics(
      new ObjectPath("default_db", "T7"),
      new CatalogColumnStatistics(columnStatisticsData),
      false)

    val ddl3 =
      """
        | CREATE TABLE catalog1.default_db.T8 (
        |  a8 int,
        |  b8 bigint,
        |  c8 string
        |  ) WITH (
        |  'connector' = 'values',
        |  %s
        |  )
        |""".stripMargin.format(getProperties)
    util.tableEnv.executeSql(ddl3)
    columnStatisticsData = new java.util.HashMap[String, CatalogColumnStatisticsDataBase]()
    catalog.alterTableStatistics(new ObjectPath("default_db", "T8"), tableStats, false)
    columnStatisticsData.put("a8", columnAStats)
    columnStatisticsData.put("b8", columnBStats)
    catalog.alterTableColumnStatistics(
      new ObjectPath("default_db", "T8"),
      new CatalogColumnStatistics(columnStatisticsData),
      false)

    util.getTableEnv.getConfig
      .set(JoinDeriveNullFilterRule.TABLE_OPTIMIZER_JOIN_NULL_FILTER_THRESHOLD, Long.box(10000))
    val sql =
      s"""
         |SELECT * FROM T6
         |   INNER JOIN T7 ON b6 = b7
         |   INNER JOIN T8 ON a6 = a8
         |""".stripMargin
    util.verifyExecPlan(sql)
  }

  def alterTableStatistics(): Unit = {
    catalog.alterTableStatistics(
      new ObjectPath("default_db", "T1"),
      new CatalogTableStatistics(1000000L, 100, 100, 100L),
      false)
    var columnStatisticsData = new java.util.HashMap[String, CatalogColumnStatisticsDataBase]()
    columnStatisticsData.put("a1", new CatalogColumnStatisticsDataLong(null, null, 1000000L, 0L))
    columnStatisticsData.put("b1", new CatalogColumnStatisticsDataLong(null, null, 10L, 0L))
    catalog.alterTableColumnStatistics(
      new ObjectPath("default_db", "T1"),
      new CatalogColumnStatistics(columnStatisticsData),
      false)

    catalog.alterTableStatistics(
      new ObjectPath("default_db", "T2"),
      new CatalogTableStatistics(10000L, 100, 100, 100L),
      false)
    columnStatisticsData = new java.util.HashMap[String, CatalogColumnStatisticsDataBase]()
    columnStatisticsData.put("a2", new CatalogColumnStatisticsDataLong(null, null, 100L, 0L))
    columnStatisticsData.put("b2", new CatalogColumnStatisticsDataLong(null, null, 5000L, 0L))
    catalog.alterTableColumnStatistics(
      new ObjectPath("default_db", "T2"),
      new CatalogColumnStatistics(columnStatisticsData),
      false)

    catalog.alterTableStatistics(
      new ObjectPath("default_db", "T3"),
      new CatalogTableStatistics(10L, 100, 100, 100L),
      false)
    columnStatisticsData = new java.util.HashMap[String, CatalogColumnStatisticsDataBase]()
    columnStatisticsData.put("a3", new CatalogColumnStatisticsDataLong(null, null, 5L, 0L))
    columnStatisticsData.put("b3", new CatalogColumnStatisticsDataLong(null, null, 2L, 0L))
    catalog.alterTableColumnStatistics(
      new ObjectPath("default_db", "T3"),
      new CatalogColumnStatistics(columnStatisticsData),
      false)

    catalog.alterTableStatistics(
      new ObjectPath("default_db", "T4"),
      new CatalogTableStatistics(100L, 100, 100, 100L),
      false)
    columnStatisticsData = new java.util.HashMap[String, CatalogColumnStatisticsDataBase]()
    columnStatisticsData.put("a4", new CatalogColumnStatisticsDataLong(null, null, 100L, 0L))
    columnStatisticsData.put("b4", new CatalogColumnStatisticsDataLong(null, null, 20L, 0L))
    catalog.alterTableColumnStatistics(
      new ObjectPath("default_db", "T4"),
      new CatalogColumnStatistics(columnStatisticsData),
      false)

    catalog.alterTableStatistics(
      new ObjectPath("default_db", "T5"),
      new CatalogTableStatistics(500000L, 100, 100, 100L),
      false)
    columnStatisticsData = new java.util.HashMap[String, CatalogColumnStatisticsDataBase]()
    columnStatisticsData.put("a5", new CatalogColumnStatisticsDataLong(null, null, 200000L, 0L))
    columnStatisticsData.put("b5", new CatalogColumnStatisticsDataLong(null, null, 20L, 0L))
    catalog.alterTableColumnStatistics(
      new ObjectPath("default_db", "T5"),
      new CatalogColumnStatistics(columnStatisticsData),
      false)

  }
}
