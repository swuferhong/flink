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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.catalog.{Catalog, ObjectPath}
import org.apache.flink.table.catalog.stats.CatalogTableStatistics
import org.apache.flink.table.planner.factories.TestValuesCatalog
import org.apache.flink.table.planner.plan.utils.NonPojo
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Before, Test}

class SetOperatorsTest extends TableTestBase {

  private val util = batchTestUtil()
  private val tEnv: TableEnvironment = util.tableEnv
  private val catalog: Catalog = new TestValuesCatalog("catalog1", "default_db", false)

  @Before
  def before(): Unit = {
    catalog.open()
    tEnv.registerCatalog("catalog1", catalog)
    tEnv.useCatalog("catalog1")
    tEnv.useDatabase("default_db")
    tEnv.getConfig.set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortAgg")

    val ddl1 =
      """
        |CREATE TABLE T1 (a int, b bigint, c string) WITH (
        | 'connector' = 'values',
        | 'bounded' = 'true'
        | )
        |""".stripMargin
    tEnv.executeSql(ddl1)
    catalog.alterTableStatistics(
      new ObjectPath("default_db", "T1"),
      new CatalogTableStatistics(100L, 10, 10L, 10L),
      false)

    val ddl2 =
      """
        |CREATE TABLE T2 (d int, e bigint, f string) WITH (
        | 'connector' = 'values',
        | 'bounded' = 'true'
        | )
        |""".stripMargin
    tEnv.executeSql(ddl2)
    catalog.alterTableStatistics(
      new ObjectPath("default_db", "T2"),
      new CatalogTableStatistics(100L, 10, 10L, 10L),
      false)

    val ddl3 =
      """
        |CREATE TABLE T3 (a int, b bigint, d int, c string, e bigint) WITH (
        | 'connector' = 'values',
        | 'bounded' = 'true'
        | )
        |""".stripMargin
    tEnv.executeSql(ddl3)
    catalog.alterTableStatistics(
      new ObjectPath("default_db", "T3"),
      new CatalogTableStatistics(100L, 10, 10L, 10L),
      false)
  }

  @Test(expected = classOf[ValidationException])
  def testUnionDifferentColumnSize(): Unit = {
    // must fail. Union inputs have different column size.
    util.verifyExecPlan("SELECT * FROM T1 UNION ALL SELECT * FROM T3")
  }

  @Test(expected = classOf[ValidationException])
  def testUnionDifferentFieldTypes(): Unit = {
    // must fail. Union inputs have different field types.
    util.verifyExecPlan("SELECT a, b, c FROM T1 UNION ALL SELECT d, c, e FROM T3")
  }

  @Test
  def testIntersectAll(): Unit = {
    util.verifyExecPlan("SELECT c FROM T1 INTERSECT ALL SELECT f FROM T2")
  }

  @Test(expected = classOf[ValidationException])
  def testIntersectDifferentFieldTypes(): Unit = {
    // must fail. Intersect inputs have different field types.
    util.verifyExecPlan("SELECT a, b, c FROM T1 INTERSECT SELECT d, c, e FROM T3")
  }

  @Test
  def testMinusAll(): Unit = {
    util.verifyExecPlan("SELECT c FROM T1 EXCEPT ALL SELECT f FROM T2")
  }

  @Test(expected = classOf[ValidationException])
  def testMinusDifferentFieldTypes(): Unit = {
    // must fail. Minus inputs have different field types.
    util.verifyExecPlan("SELECT a, b, c FROM T1 EXCEPT SELECT d, c, e FROM T3")
  }

  @Test
  def testIntersect(): Unit = {
    util.verifyExecPlan("SELECT c FROM T1 INTERSECT SELECT f FROM T2")
  }

  @Test
  def testIntersectLeftIsEmpty(): Unit = {
    util.verifyExecPlan("SELECT c FROM T1 WHERE 1=0 INTERSECT SELECT f FROM T2")
  }

  @Test
  def testIntersectRightIsEmpty(): Unit = {
    util.verifyExecPlan("SELECT c FROM T1 INTERSECT SELECT f FROM T2 WHERE 1=0")
  }

  @Test
  def testMinus(): Unit = {
    util.verifyExecPlan("SELECT c FROM T1 EXCEPT SELECT f FROM T2")
  }

  @Test
  def testMinusLeftIsEmpty(): Unit = {
    util.verifyExecPlan("SELECT c FROM T1 WHERE 1=0 EXCEPT SELECT f FROM T2")
  }

  @Test
  def testMinusRightIsEmpty(): Unit = {
    util.verifyExecPlan("SELECT c FROM T1 EXCEPT SELECT f FROM T2 WHERE 1=0")
  }

  @Test
  def testMinusWithNestedTypes(): Unit = {
    val ddl =
      """
        |CREATE TABLE MyTable (a bigint, b row(b1 int, b2 string), c array<boolean>) WITH (
        | 'connector' = 'values',
        | 'bounded' = 'true'
        | )
        |""".stripMargin
    tEnv.executeSql(ddl)
    catalog.alterTableStatistics(
      new ObjectPath("default_db", "MyTable"),
      new CatalogTableStatistics(100L, 10, 10L, 10L),
      false)
    util.verifyExecPlan("SELECT * FROM MyTable EXCEPT SELECT * FROM MyTable")
  }

  @Test
  def testUnionNullableTypes(): Unit = {
    val ddl =
      """
        |CREATE TABLE A (a row(a1 int, a2 string), b row(b1 int, b2 string), c int) WITH (
        | 'connector' = 'values',
        | 'bounded' = 'true'
        | )
        |""".stripMargin
    tEnv.executeSql(ddl)
    catalog.alterTableStatistics(
      new ObjectPath("default_db", "A"),
      new CatalogTableStatistics(100L, 10, 10L, 10L),
      false)
    util.verifyExecPlan(
      "SELECT a FROM A UNION ALL SELECT CASE WHEN c > 0 THEN b ELSE NULL END FROM A")
  }

  @Test
  def testUnionAnyType(): Unit = {
    val util = batchTestUtil()
    util.addTableSource(
      "A",
      Array[TypeInformation[_]](
        new GenericTypeInfo(classOf[NonPojo]),
        new GenericTypeInfo(classOf[NonPojo])),
      Array("a", "b"))
    util.verifyExecPlan("SELECT a FROM A UNION ALL SELECT b FROM A")
  }

  @Test
  def testIntersectWithOuterProject(): Unit = {
    util.verifyExecPlan("SELECT a FROM (SELECT a, b FROM T1 INTERSECT SELECT d, e FROM T2)")
  }
}
