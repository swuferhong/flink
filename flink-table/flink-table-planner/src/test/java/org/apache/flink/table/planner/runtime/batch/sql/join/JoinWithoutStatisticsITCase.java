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

package org.apache.flink.table.planner.runtime.batch.sql.join;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.planner.factories.TestValuesCatalog;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * ITCase for table join while partial tables or all tables without Statistics. For now, if table
 * without statistics in batch mode. Join reorder, BroadCast HashJoin and HashJoin will be disabled.
 */
public class JoinWithoutStatisticsITCase extends BatchTestBase {

    private TableEnvironment tEnv;
    private final Catalog catalog = new TestValuesCatalog("catalog1", "default_db", false);

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();
        tEnv = tEnv();
        catalog.open();
        tEnv.registerCatalog("catalog1", catalog);
        tEnv.useCatalog("catalog1");
        tEnv.useDatabase("default_db");
        String dataId1 = TestValuesTableFactory.registerData(TestData.smallData3());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T1 (\n"
                                + " `a` int,\n"
                                + " `b` bigint,\n"
                                + " `c` string\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n "
                                + " 'bounded' = 'true'\n "
                                + ")",
                        dataId1));

        String dataId2 = TestValuesTableFactory.registerData(TestData.data2());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T2 (\n"
                                + " `a` int,\n"
                                + " `b` bigint,\n"
                                + " `c` int,\n"
                                + " `d` string,\n"
                                + " `e` bigint\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n "
                                + " 'bounded' = 'true'\n "
                                + ")",
                        dataId2));
    }

    @Test
    public void testInnerJoinWithoutStatistics() {
        checkResult(
                "SELECT T1.c, T2.d FROM T1 JOIN T2 ON T1.a = T2.a WHERE T2.a = 2",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of("Hello", "Hallo Welt wie"), Row.of("Hello", "Hallo Welt"))),
                false);
    }

    @Test
    public void testInnerJoinReorderWithPartialTableNoStatistics() throws TableNotExistException {
        String dataId = TestValuesTableFactory.registerData(TestData.data5());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T3 (\n"
                                + " `a` int,\n"
                                + " `b` bigint,\n"
                                + " `c` int,\n"
                                + " `d` string,\n"
                                + " `e` bigint\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n "
                                + " 'bounded' = 'true'\n "
                                + ")",
                        dataId));
        catalog.alterTableStatistics(
                new ObjectPath("default_db", "T1"),
                new CatalogTableStatistics(100L, 10, 10L, 10L),
                false);
        catalog.alterTableStatistics(
                new ObjectPath("default_db", "T2"),
                new CatalogTableStatistics(10L, 10, 10L, 10L),
                false);

        checkResult(
                "SELECT T3.d, T1.c, T2.d FROM T3 JOIN T1 ON T1.a = T3.a JOIN T2 ON T1.a = T2.a WHERE T2.a = 2",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of("Hallo Welt wie", "Hello", "Hallo Welt wie"),
                                Row.of("Hallo Welt wie", "Hello", "Hallo Welt"),
                                Row.of("Hallo Welt", "Hello", "Hallo Welt wie"),
                                Row.of("Hallo Welt", "Hello", "Hallo Welt"))),
                false);
    }

    @Test
    public void testInnerJoinReorderWithAllTableNoStatistics() {
        String dataId = TestValuesTableFactory.registerData(TestData.data5());
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE T3 (\n"
                                + " `a` int,\n"
                                + " `b` bigint,\n"
                                + " `c` int,\n"
                                + " `d` string,\n"
                                + " `e` bigint\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'data-id' = '%s',\n "
                                + " 'bounded' = 'true'\n "
                                + ")",
                        dataId));

        checkResult(
                "SELECT T3.d, T1.c, T2.d FROM T3 JOIN T1 ON T1.a = T3.a JOIN T2 ON T1.a = T2.a WHERE T2.a = 2",
                JavaScalaConversionUtil.toScala(
                        Arrays.asList(
                                Row.of("Hallo Welt wie", "Hello", "Hallo Welt wie"),
                                Row.of("Hallo Welt wie", "Hello", "Hallo Welt"),
                                Row.of("Hallo Welt", "Hello", "Hallo Welt wie"),
                                Row.of("Hallo Welt", "Hello", "Hallo Welt"))),
                false);
    }

    @AfterEach
    @Override
    public void after() {
        TestValuesTableFactory.clearAllData();
    }
}
