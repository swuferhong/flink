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
package org.apache.flink.table.planner.plan.batch.sql

import org.apache.flink.table.api._
import org.apache.flink.table.catalog.ObjectPath
import org.apache.flink.table.catalog.stats.CatalogTableStatistics
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Before, Test}

class WindowTableFunctionTest extends TableTestBase {

  private val util = batchTestUtil()
  private val tEnv: TableEnvironment = util.tableEnv

  @Before
  def before(): Unit = {
    val ddl1 =
      """
        |CREATE TABLE MyTable (ts timestamp, a bigint, b int, c string) WITH (
        | 'connector' = 'values',
        | 'bounded' = 'true'
        | )
        |""".stripMargin
    tEnv.executeSql(ddl1)
    util.tableEnv
      .getCatalog("default_catalog")
      .get()
      .alterTableStatistics(
        new ObjectPath("default_database", "MyTable"),
        new CatalogTableStatistics(100000001L, 10, 10L, 10L),
        false)

    val ddl2 =
      """
        |CREATE TABLE MyTable1 (a int, b bigint, c string, d int, ts timestamp) WITH (
        | 'connector' = 'values',
        | 'bounded' = 'true'
        | )
        |""".stripMargin
    tEnv.executeSql(ddl2)
    util.tableEnv
      .getCatalog("default_catalog")
      .get()
      .alterTableStatistics(
        new ObjectPath("default_database", "MyTable1"),
        new CatalogTableStatistics(100000001L, 10, 10L, 10L),
        false)

    tEnv.executeSql(s"""
                       |create table MyTable2 (
                       |  a int,
                       |  b bigint,
                       |  c as proctime()
                       |) with (
                       |  'connector' = 'COLLECTION'
                       |)
                       |""".stripMargin)
  }

  @Test
  def testInvalidTimeColType(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(TUMBLE(TABLE MyTable1, DESCRIPTOR(b), INTERVAL '15' MINUTE))
        |""".stripMargin
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage(
      "The window function TUMBLE(TABLE table_name, DESCRIPTOR(timecol), datetime interval"
        + "[, datetime interval]) requires the timecol to be TIMESTAMP or TIMESTAMP_LTZ, "
        + "but is BIGINT.")
    util.verifyExplain(sql)
  }

  @Test
  def testTumbleTVF(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(TUMBLE(TABLE MyTable1, DESCRIPTOR(ts), INTERVAL '15' MINUTE))
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testTumbleTVFProctime(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(TUMBLE(TABLE MyTable2, DESCRIPTOR(c), INTERVAL '15' MINUTE))
        |""".stripMargin
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage("Processing time Window TableFunction is not supported yet.")
    util.verifyExplain(sql)
  }

  @Test
  def testHopTVF(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(HOP(TABLE MyTable1, DESCRIPTOR(ts), INTERVAL '1' HOUR, INTERVAL '2' HOUR))
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testHopTVFProctime(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(HOP(TABLE MyTable2, DESCRIPTOR(c), INTERVAL '1' HOUR, INTERVAL '2' HOUR))
        |""".stripMargin
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage("Processing time Window TableFunction is not supported yet.")
    util.verifyExplain(sql)
  }

  @Test
  def testCumulateTVF(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(
        | CUMULATE(TABLE MyTable1, DESCRIPTOR(ts), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testCumulateTVFProctime(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM TABLE(
        | CUMULATE(TABLE MyTable2, DESCRIPTOR(c), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |""".stripMargin
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage("Processing time Window TableFunction is not supported yet.")
    util.verifyExplain(sql)
  }

  @Test
  def testWindowAggregate(): Unit = {
    val sql =
      """
        |SELECT
        |  window_start,
        |  window_end,
        |  a,
        |  MAX(c)
        |FROM TABLE(TUMBLE(TABLE MyTable1, DESCRIPTOR(ts), INTERVAL '3' SECOND))
        |GROUP BY window_start, window_end, a
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testCascadingWindowAggregate(): Unit = {
    val sql =
      """
        |SELECT window_start, window_end, b, SUM(cnt)
        |FROM (
        |  SELECT
        |    window_start, window_end, a, b, COUNT(1) AS cnt
        |  FROM TABLE(TUMBLE(TABLE MyTable1, DESCRIPTOR(ts), INTERVAL '3' SECOND))
        |  GROUP BY window_start, window_end, a, b
        |)
        |GROUP BY window_start, window_end, b
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testWindowJoin(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(ts), INTERVAL '15' MINUTE))
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE MyTable1, DESCRIPTOR(ts), INTERVAL '15' MINUTE))
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a = R.b
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testWindowRank(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |SELECT *,
        |  RANK() OVER(PARTITION BY a, window_start, window_end ORDER BY b) as rownum
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(ts), INTERVAL '15' MINUTE))
        |)
        |WHERE rownum <= 3
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testProjectWTFTransposeRule(): Unit = {
    val sql =
      """
        |SELECT
        |  MAX(c)
        |FROM TABLE(TUMBLE(TABLE MyTable1, DESCRIPTOR(ts), INTERVAL '3' SECOND))
        |GROUP BY window_start, window_end, a
        |""".stripMargin
    util.verifyExecPlan(sql)
  }
}
