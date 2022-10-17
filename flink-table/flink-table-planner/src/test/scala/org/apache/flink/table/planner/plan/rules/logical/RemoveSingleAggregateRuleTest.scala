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

import org.apache.flink.table.api._
import org.apache.flink.table.catalog.ObjectPath
import org.apache.flink.table.catalog.stats.CatalogTableStatistics
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Before, Test}

/** Test for RemoveSingleAggregateRule. */
class RemoveSingleAggregateRuleTest extends TableTestBase {

  private val util = batchTestUtil()
  private val tEnv: TableEnvironment = util.tableEnv

  @Before
  def setup(): Unit = {
    val ddl1 =
      """
        |CREATE TABLE foo (x int, y int) WITH (
        | 'connector' = 'values',
        | 'bounded' = 'true'
        | )
        |""".stripMargin
    tEnv.executeSql(ddl1)
    tEnv
      .getCatalog("default_catalog")
      .get()
      .alterTableStatistics(
        new ObjectPath("default_database", "foo"),
        new CatalogTableStatistics(100000001L, 10, 10L, 10L),
        false)

    val ddl2 =
      """
        |CREATE TABLE bar (i int, s string) WITH (
        | 'connector' = 'values',
        | 'bounded' = 'true'
        | )
        |""".stripMargin
    tEnv.executeSql(ddl2)
    tEnv
      .getCatalog("default_catalog")
      .get()
      .alterTableStatistics(
        new ObjectPath("default_database", "bar"),
        new CatalogTableStatistics(100000001L, 10, 10L, 10L),
        false)
  }

  @Test
  def testRemoveSingleAggregateRule(): Unit = {
    util.verifyRelPlan("select (select count(x)-1 from foo where foo.y=bar.i) from bar")
  }

}
