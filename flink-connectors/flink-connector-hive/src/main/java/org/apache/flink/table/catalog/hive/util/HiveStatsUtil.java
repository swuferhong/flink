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

package org.apache.flink.table.catalog.hive.util;

import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBinary;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.Date;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utils for stats of HiveCatalog. */
public class HiveStatsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HiveStatsUtil.class);

    private static final int DEFAULT_UNKNOWN_STATS_VALUE = -1;

    private HiveStatsUtil() {}

    /** Create a map of Flink column stats from the given Hive column stats. */
    public static Map<String, CatalogColumnStatisticsDataBase> createCatalogColumnStats(
            @Nonnull List<ColumnStatisticsObj> hiveColStats, String hiveVersion) {
        checkNotNull(hiveColStats, "hiveColStats can not be null");
        Map<String, CatalogColumnStatisticsDataBase> colStats = new HashMap<>();
        for (ColumnStatisticsObj colStatsObj : hiveColStats) {
            CatalogColumnStatisticsDataBase columnStats =
                    createTableColumnStats(
                            HiveTypeUtil.toFlinkType(
                                    TypeInfoUtils.getTypeInfoFromTypeString(
                                            colStatsObj.getColType())),
                            colStatsObj.getStatsData(),
                            hiveVersion);
            colStats.put(colStatsObj.getColName(), columnStats);
        }

        return colStats;
    }

    /**
     * Create a map of Flink column stats from the given Hive partition and columns statistic of the
     * partition.
     *
     * <p>The basic idea is to merge the column's statistic of all partitions to get completed
     * statistic for one column.
     */
    public static Map<String, CatalogColumnStatisticsDataBase> createCatalogColumnStats(
            Map<String, List<ColumnStatisticsObj>> partitionColumnStatistics,
            String hiveVersion,
            HiveCatalog hiveCatalog,
            ObjectPath tablePath) {
        // column -> the column statistic of all partitions
        Map<String, List<CatalogColumnStatisticsDataBase>> colPartitionStats = new HashMap<>();
        // column -> partition that the column statistic belongs
        // the mapping is need to when we merge the avgLength statistic
        Map<String, List<String>> columnPartitions = new HashMap<>();

        // iterate each partition and the columns' statistic
        for (Map.Entry<String, List<ColumnStatisticsObj>> partitionColumnStatisticsEntry :
                partitionColumnStatistics.entrySet()) {
            String partition = partitionColumnStatisticsEntry.getKey();
            List<ColumnStatisticsObj> columnStatisticsObjList =
                    partitionColumnStatisticsEntry.getValue();
            // iterate each column to get statistic
            for (ColumnStatisticsObj colStatsObj : columnStatisticsObjList) {
                // create catalog column statistic
                CatalogColumnStatisticsDataBase columnStats =
                        createTableColumnStats(
                                HiveTypeUtil.toFlinkType(
                                        TypeInfoUtils.getTypeInfoFromTypeString(
                                                colStatsObj.getColType())),
                                colStatsObj.getStatsData(),
                                hiveVersion);
                // record the column and the corresponding column statistic
                colPartitionStats.putIfAbsent(colStatsObj.getColName(), new ArrayList<>());
                colPartitionStats.get(colStatsObj.getColName()).add(columnStats);
                // record the column and partition that the column statistic belongs,
                columnPartitions.putIfAbsent(colStatsObj.getColName(), new ArrayList<>());
                columnPartitions.get(colStatsObj.getColName()).add(partition);
            }
        }

        // column -> merged column statistic
        Map<String, CatalogColumnStatisticsDataBase> colStats = new HashMap<>();
        // partition -> row count in partition
        Map<String, Long> cachePartitionRowCount = new HashMap<>();
        LOG.info("PARTITION TABLE STATISTICS FOR COLUMNS-----------------------------------------");
        colPartitionStats.forEach(
                (colName, colPartitionStatisticList) ->
                        colStats.put(
                                colName,
                                mergeCatalogTableColumnStats(
                                        colPartitionStatisticList,
                                        columnPartitions.get(colName),
                                        hiveCatalog,
                                        cachePartitionRowCount,
                                        tablePath,
                                        colName)));
        return colStats;
    }

    /** Create columnStatistics from the given Hive column stats of a hive table. */
    public static ColumnStatistics createTableColumnStats(
            Table hiveTable,
            Map<String, CatalogColumnStatisticsDataBase> colStats,
            String hiveVersion) {
        ColumnStatisticsDesc desc =
                new ColumnStatisticsDesc(true, hiveTable.getDbName(), hiveTable.getTableName());
        return createHiveColumnStatistics(colStats, hiveTable.getSd(), desc, hiveVersion);
    }

    /** Create columnStatistics from the given Hive column stats of a hive partition. */
    public static ColumnStatistics createPartitionColumnStats(
            Partition hivePartition,
            String partName,
            Map<String, CatalogColumnStatisticsDataBase> colStats,
            String hiveVersion) {
        ColumnStatisticsDesc desc =
                new ColumnStatisticsDesc(
                        false, hivePartition.getDbName(), hivePartition.getTableName());
        desc.setPartName(partName);
        return createHiveColumnStatistics(colStats, hivePartition.getSd(), desc, hiveVersion);
    }

    private static ColumnStatistics createHiveColumnStatistics(
            Map<String, CatalogColumnStatisticsDataBase> colStats,
            StorageDescriptor sd,
            ColumnStatisticsDesc desc,
            String hiveVersion) {
        List<ColumnStatisticsObj> colStatsList = new ArrayList<>();

        for (FieldSchema field : sd.getCols()) {
            String hiveColName = field.getName();
            String hiveColType = field.getType();
            CatalogColumnStatisticsDataBase flinkColStat = colStats.get(field.getName());
            if (null != flinkColStat) {
                ColumnStatisticsData statsData =
                        getColumnStatisticsData(
                                HiveTypeUtil.toFlinkType(
                                        TypeInfoUtils.getTypeInfoFromTypeString(hiveColType)),
                                flinkColStat,
                                hiveVersion);
                ColumnStatisticsObj columnStatisticsObj =
                        new ColumnStatisticsObj(hiveColName, hiveColType, statsData);
                colStatsList.add(columnStatisticsObj);
            }
        }
        return new ColumnStatistics(desc, colStatsList);
    }

    private static CatalogColumnStatisticsDataBase mergeCatalogTableColumnStats(
            List<CatalogColumnStatisticsDataBase> columnStatisticsDataList,
            List<String> partitions,
            HiveCatalog hiveCatalog,
            Map<String, Long> cachePartitionRowCount,
            ObjectPath tablePath,
            String colName) {
        Preconditions.checkArgument(columnStatisticsDataList.size() > 0);
        CatalogColumnStatisticsDataBase colStat = columnStatisticsDataList.get(0);
        if (colStat instanceof CatalogColumnStatisticsDataBinary) {
            CatalogColumnStatisticsDataBase cdb =
                    mergeCatalogColumnStatisticsDataBinary(
                            columnStatisticsDataList,
                            partitions,
                            hiveCatalog,
                            cachePartitionRowCount,
                            tablePath);
            CatalogColumnStatisticsDataBinary cdb1 = (CatalogColumnStatisticsDataBinary) cdb;
            LOG.info("merged statistics : colName: {}  statistics: {}", colName, cdb1);
            return cdb;
        } else if (colStat instanceof CatalogColumnStatisticsDataBoolean) {
            CatalogColumnStatisticsDataBase cdb =
                    mergeCatalogColumnStatisticsDataBoolean(columnStatisticsDataList);
            CatalogColumnStatisticsDataBoolean cdb1 = (CatalogColumnStatisticsDataBoolean) cdb;
            LOG.info("merged statistics : colName: {}  statistics: {}", colName, cdb1);
            return cdb;
        } else if (colStat instanceof CatalogColumnStatisticsDataDate) {
            CatalogColumnStatisticsDataBase cdb =
                    mergeCatalogColumnStatisticsDataDate(columnStatisticsDataList);
            CatalogColumnStatisticsDataDate cdd = (CatalogColumnStatisticsDataDate) cdb;
            LOG.info("merged statistics : colName: {}  statistics: {}", colName, cdd);
            return cdb;
        } else if (colStat instanceof CatalogColumnStatisticsDataDouble) {
            CatalogColumnStatisticsDataBase cdb =
                    mergeCatalogColumnStatisticsDataDouble(columnStatisticsDataList);
            CatalogColumnStatisticsDataDouble cdd = (CatalogColumnStatisticsDataDouble) cdb;
            LOG.info("merged statistics : colName: {}  statistics: {}", colName, cdd);
            return cdb;
        } else if (colStat instanceof CatalogColumnStatisticsDataLong) {
            CatalogColumnStatisticsDataBase cdb =
                    mergeCatalogColumnStatisticsDataLong(columnStatisticsDataList);
            CatalogColumnStatisticsDataLong cdl = (CatalogColumnStatisticsDataLong) cdb;
            LOG.info("merged statistics : colName: {}  statistics: {}", colName, cdl);
            return cdb;
        } else if (colStat instanceof CatalogColumnStatisticsDataString) {
            CatalogColumnStatisticsDataBase cdb =
                    mergeCatalogColumnStatisticsDataString(
                            columnStatisticsDataList,
                            partitions,
                            hiveCatalog,
                            cachePartitionRowCount,
                            tablePath);
            CatalogColumnStatisticsDataString cds = (CatalogColumnStatisticsDataString) cdb;
            LOG.info("merged statistics : colName: {}  statistics: {}", colName, cds);
            return cdb;
        } else {
            return null;
        }
    }

    private static Long getPartitionRowCount(
            Map<String, Long> cachePartitionRowCountMap,
            String partition,
            HiveCatalog hiveCatalog,
            ObjectPath objectPath) {
        if (!cachePartitionRowCountMap.containsKey(partition)) {
            Long rowCount;
            try {
                rowCount =
                        hiveCatalog
                                .getPartitionStatistics(
                                        objectPath, HiveCatalog.createPartitionSpec(partition))
                                .getRowCount();
            } catch (Exception e) {
                LOG.info(
                        String.format(
                                "Can't get row count for partition %s, so return null directly.",
                                partition),
                        e);
                rowCount = null;
            }
            cachePartitionRowCountMap.put(partition, rowCount);
        }
        return cachePartitionRowCountMap.get(partition);
    }

    private static CatalogColumnStatisticsDataBase mergeCatalogColumnStatisticsDataBinary(
            List<CatalogColumnStatisticsDataBase> columnStatisticsDataList,
            List<String> partitions,
            HiveCatalog hiveCatalog,
            Map<String, Long> cachePartitionRowCountMap,
            ObjectPath tablePath) {
        CatalogColumnStatisticsDataBinary colStat =
                (CatalogColumnStatisticsDataBinary) columnStatisticsDataList.get(0);
        Long maxLength = colStat.getMaxLength();
        Double avgLength = colStat.getAvgLength();
        Long currentRowsCount =
                getPartitionRowCount(
                        cachePartitionRowCountMap, partitions.get(0), hiveCatalog, tablePath);
        Long nullCount = colStat.getNullCount();
        Map<String, String> props = new HashMap<>(colStat.getProperties());
        for (int i = 1; i < columnStatisticsDataList.size(); i++) {
            CatalogColumnStatisticsDataBinary anotherColStat =
                    (CatalogColumnStatisticsDataBinary) columnStatisticsDataList.get(i);
            maxLength = max(maxLength, anotherColStat.getMaxLength());
            Long anotherRowsCount =
                    getPartitionRowCount(
                            cachePartitionRowCountMap, partitions.get(i), hiveCatalog, tablePath);
            avgLength =
                    average(
                            avgLength,
                            currentRowsCount,
                            anotherColStat.getAvgLength(),
                            anotherRowsCount);
            currentRowsCount += anotherRowsCount;
            nullCount = add(nullCount, anotherColStat.getNullCount());
            props.putAll(anotherColStat.getProperties());
        }

        return new CatalogColumnStatisticsDataBinary(maxLength, avgLength, nullCount, props);
    }

    private static CatalogColumnStatisticsDataBase mergeCatalogColumnStatisticsDataBoolean(
            List<CatalogColumnStatisticsDataBase> columnStatisticsDataList) {
        CatalogColumnStatisticsDataBoolean colStat =
                (CatalogColumnStatisticsDataBoolean) columnStatisticsDataList.get(0);
        Long trueCount = colStat.getTrueCount();
        Long falseCount = colStat.getFalseCount();
        Long nullCount = colStat.getNullCount();
        Map<String, String> props = new HashMap<>(colStat.getProperties());
        for (int i = 1; i < columnStatisticsDataList.size(); i++) {
            CatalogColumnStatisticsDataBoolean anotherColStat =
                    (CatalogColumnStatisticsDataBoolean) columnStatisticsDataList.get(i);
            trueCount = add(trueCount, anotherColStat.getTrueCount());
            falseCount = add(falseCount, anotherColStat.getFalseCount());
            nullCount = add(nullCount, anotherColStat.getNullCount());
            props.putAll(anotherColStat.getProperties());
        }

        return new CatalogColumnStatisticsDataBoolean(trueCount, falseCount, nullCount, props);
    }

    private static CatalogColumnStatisticsDataBase mergeCatalogColumnStatisticsDataString(
            List<CatalogColumnStatisticsDataBase> columnStatisticsDataList,
            List<String> partitions,
            HiveCatalog hiveCatalog,
            Map<String, Long> cachePartitionRowCountMap,
            ObjectPath tablePath) {
        CatalogColumnStatisticsDataString colStat =
                (CatalogColumnStatisticsDataString) columnStatisticsDataList.get(0);
        Long maxLength = colStat.getMaxLength();
        Double avgLength = colStat.getAvgLength();
        Long ndv = colStat.getNdv();
        Long nullCount = colStat.getNullCount();
        Long currentRowsCount =
                getPartitionRowCount(
                        cachePartitionRowCountMap, partitions.get(0), hiveCatalog, tablePath);
        Map<String, String> props = new HashMap<>(colStat.getProperties());
        for (int i = 1; i < columnStatisticsDataList.size(); i++) {
            CatalogColumnStatisticsDataString anotherColStat =
                    (CatalogColumnStatisticsDataString) columnStatisticsDataList.get(i);
            maxLength = max(maxLength, anotherColStat.getMaxLength());
            Long anotherRowsCount =
                    getPartitionRowCount(
                            cachePartitionRowCountMap, partitions.get(i), hiveCatalog, tablePath);
            avgLength =
                    average(
                            avgLength,
                            currentRowsCount,
                            anotherColStat.getAvgLength(),
                            anotherRowsCount);
            currentRowsCount += anotherRowsCount;
            // note: currently, we have no way to get ndv according the statistic from every single
            // partition, so we just sum the ndv, it may be inaccuracy
            ndv = max(ndv, anotherColStat.getNdv());
            nullCount = add(nullCount, anotherColStat.getNullCount());
            props.putAll(anotherColStat.getProperties());
        }
        return new CatalogColumnStatisticsDataString(maxLength, avgLength, ndv, nullCount, props);
    }

    private static CatalogColumnStatisticsDataBase mergeCatalogColumnStatisticsDataLong(
            List<CatalogColumnStatisticsDataBase> columnStatisticsDataList) {
        CatalogColumnStatisticsDataLong colStat =
                (CatalogColumnStatisticsDataLong) columnStatisticsDataList.get(0);
        Long min = colStat.getMin();
        Long max = colStat.getMax();
        Long ndv = colStat.getNdv();
        Long nullCount = colStat.getNullCount();
        Map<String, String> props = new HashMap<>(colStat.getProperties());
        for (int i = 1; i < columnStatisticsDataList.size(); i++) {
            CatalogColumnStatisticsDataLong anotherColStat =
                    (CatalogColumnStatisticsDataLong) columnStatisticsDataList.get(i);
            min = min(min, anotherColStat.getMin());
            max = max(max, anotherColStat.getMax());
            // note: currently, we have no way to get ndv according the statistic from every single
            // partition, so we just sum the ndv, it may be inaccuracy
            ndv = max(ndv, anotherColStat.getNdv());
            nullCount = add(nullCount, anotherColStat.getNullCount());
            props.putAll(anotherColStat.getProperties());
        }
        return new CatalogColumnStatisticsDataLong(min, max, ndv, nullCount, props);
    }

    private static CatalogColumnStatisticsDataBase mergeCatalogColumnStatisticsDataDouble(
            List<CatalogColumnStatisticsDataBase> columnStatisticsDataList) {
        CatalogColumnStatisticsDataDouble colStat =
                (CatalogColumnStatisticsDataDouble) columnStatisticsDataList.get(0);
        Double min = colStat.getMin();
        Double max = colStat.getMax();
        Long ndv = colStat.getNdv();
        Long nullCount = colStat.getNullCount();
        Map<String, String> props = new HashMap<>(colStat.getProperties());
        for (int i = 1; i < columnStatisticsDataList.size(); i++) {
            CatalogColumnStatisticsDataDouble anotherColStat =
                    (CatalogColumnStatisticsDataDouble) columnStatisticsDataList.get(i);
            min = min(min, anotherColStat.getMin());
            max = max(max, anotherColStat.getMax());
            // note: currently, we have no way to get ndv according the statistic from every single
            // partition, so we just sum the ndv, it may be inaccuracy
            ndv = max(ndv, anotherColStat.getNdv());
            nullCount = add(nullCount, anotherColStat.getNullCount());
            props.putAll(anotherColStat.getProperties());
        }
        return new CatalogColumnStatisticsDataDouble(min, max, ndv, nullCount, props);
    }

    private static CatalogColumnStatisticsDataBase mergeCatalogColumnStatisticsDataDate(
            List<CatalogColumnStatisticsDataBase> columnStatisticsDataList) {
        CatalogColumnStatisticsDataDate colStat =
                (CatalogColumnStatisticsDataDate) columnStatisticsDataList.get(0);
        Date min = colStat.getMin();
        Date max = colStat.getMax();
        Long ndv = colStat.getNdv();
        Long nullCount = colStat.getNullCount();
        Map<String, String> props = new HashMap<>();
        for (int i = 1; i < columnStatisticsDataList.size(); i++) {
            CatalogColumnStatisticsDataDate anotherColStat =
                    (CatalogColumnStatisticsDataDate) columnStatisticsDataList.get(i);
            min = min(min, anotherColStat.getMin());
            max = max(max, anotherColStat.getMax());
            // note: currently, we have no way to get ndv according the statistic from every single
            // partition,
            // so we just sum the ndv, it may be inaccuracy
            ndv = max(ndv, anotherColStat.getNdv());
            nullCount = add(nullCount, anotherColStat.getNullCount());
            props.putAll(anotherColStat.getProperties());
        }
        return new CatalogColumnStatisticsDataDate(min, max, ndv, nullCount, props);
    }

    /** Create Flink ColumnStats from Hive ColumnStatisticsData. */
    private static CatalogColumnStatisticsDataBase createTableColumnStats(
            DataType colType, ColumnStatisticsData stats, String hiveVersion) {
        HiveShim hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
        if (stats.isSetBinaryStats()) {
            BinaryColumnStatsData binaryStats = stats.getBinaryStats();
            return new CatalogColumnStatisticsDataBinary(
                    binaryStats.isSetMaxColLen() ? binaryStats.getMaxColLen() : null,
                    binaryStats.isSetAvgColLen() ? binaryStats.getAvgColLen() : null,
                    binaryStats.isSetNumNulls() ? binaryStats.getNumNulls() : null);
        } else if (stats.isSetBooleanStats()) {
            BooleanColumnStatsData booleanStats = stats.getBooleanStats();
            return new CatalogColumnStatisticsDataBoolean(
                    booleanStats.isSetNumTrues() ? booleanStats.getNumTrues() : null,
                    booleanStats.isSetNumFalses() ? booleanStats.getNumFalses() : null,
                    booleanStats.isSetNumNulls() ? booleanStats.getNumNulls() : null);
        } else if (hiveShim.isDateStats(stats)) {
            return hiveShim.toFlinkDateColStats(stats);
        } else if (stats.isSetDoubleStats()) {
            DoubleColumnStatsData doubleStats = stats.getDoubleStats();
            return new CatalogColumnStatisticsDataDouble(
                    doubleStats.isSetLowValue() ? doubleStats.getLowValue() : null,
                    doubleStats.isSetHighValue() ? doubleStats.getHighValue() : null,
                    doubleStats.isSetNumDVs() ? doubleStats.getNumDVs() : null,
                    doubleStats.isSetNumNulls() ? doubleStats.getNumNulls() : null);
        } else if (stats.isSetLongStats()) {
            LongColumnStatsData longColStats = stats.getLongStats();
            return new CatalogColumnStatisticsDataLong(
                    longColStats.isSetLowValue() ? longColStats.getLowValue() : null,
                    longColStats.isSetHighValue() ? longColStats.getHighValue() : null,
                    longColStats.isSetNumDVs() ? longColStats.getNumDVs() : null,
                    longColStats.isSetNumNulls() ? longColStats.getNumNulls() : null);
        } else if (stats.isSetStringStats()) {
            StringColumnStatsData stringStats = stats.getStringStats();
            return new CatalogColumnStatisticsDataString(
                    stringStats.isSetMaxColLen() ? stringStats.getMaxColLen() : null,
                    stringStats.isSetAvgColLen() ? stringStats.getAvgColLen() : null,
                    stringStats.isSetNumDVs() ? stringStats.getNumDVs() : null,
                    stringStats.isSetNumNulls() ? stringStats.getNumNulls() : null);
        } else if (stats.isSetDecimalStats()) {
            DecimalColumnStatsData decimalStats = stats.getDecimalStats();
            // for now, just return CatalogColumnStatisticsDataDouble for decimal columns
            Double max = null;
            if (decimalStats.isSetHighValue()) {
                max = toHiveDecimal(decimalStats.getHighValue()).doubleValue();
            }
            Double min = null;
            if (decimalStats.isSetLowValue()) {
                min = toHiveDecimal(decimalStats.getLowValue()).doubleValue();
            }
            Long ndv = decimalStats.isSetNumDVs() ? decimalStats.getNumDVs() : null;
            Long nullCount = decimalStats.isSetNumNulls() ? decimalStats.getNumNulls() : null;
            return new CatalogColumnStatisticsDataDouble(min, max, ndv, nullCount);
        } else {
            LOG.warn(
                    "Flink does not support converting ColumnStatisticsData '{}' for Hive column type '{}' yet.",
                    stats,
                    colType);
            return null;
        }
    }

    /**
     * Convert Flink ColumnStats to Hive ColumnStatisticsData according to Hive column type. Note we
     * currently assume that, in Flink, the max and min of ColumnStats will be same type as the
     * Flink column type. For example, for SHORT and Long columns, the max and min of their
     * ColumnStats should be of type SHORT and LONG.
     */
    private static ColumnStatisticsData getColumnStatisticsData(
            DataType colType, CatalogColumnStatisticsDataBase colStat, String hiveVersion) {
        LogicalTypeRoot type = colType.getLogicalType().getTypeRoot();
        if (type.equals(LogicalTypeRoot.CHAR) || type.equals(LogicalTypeRoot.VARCHAR)) {
            if (colStat instanceof CatalogColumnStatisticsDataString) {
                CatalogColumnStatisticsDataString stringColStat =
                        (CatalogColumnStatisticsDataString) colStat;
                StringColumnStatsData hiveStringColumnStats = new StringColumnStatsData();
                hiveStringColumnStats.clear();
                if (null != stringColStat.getMaxLength()) {
                    hiveStringColumnStats.setMaxColLen(stringColStat.getMaxLength());
                }
                if (null != stringColStat.getAvgLength()) {
                    hiveStringColumnStats.setAvgColLen(stringColStat.getAvgLength());
                }
                if (null != stringColStat.getNullCount()) {
                    hiveStringColumnStats.setNumNulls(stringColStat.getNullCount());
                }
                if (null != stringColStat.getNdv()) {
                    hiveStringColumnStats.setNumDVs(stringColStat.getNdv());
                }
                return ColumnStatisticsData.stringStats(hiveStringColumnStats);
            }
        } else if (type.equals(LogicalTypeRoot.BOOLEAN)) {
            if (colStat instanceof CatalogColumnStatisticsDataBoolean) {
                CatalogColumnStatisticsDataBoolean booleanColStat =
                        (CatalogColumnStatisticsDataBoolean) colStat;
                BooleanColumnStatsData hiveBoolStats = new BooleanColumnStatsData();
                hiveBoolStats.clear();
                if (null != booleanColStat.getTrueCount()) {
                    hiveBoolStats.setNumTrues(booleanColStat.getTrueCount());
                }
                if (null != booleanColStat.getFalseCount()) {
                    hiveBoolStats.setNumFalses(booleanColStat.getFalseCount());
                }
                if (null != booleanColStat.getNullCount()) {
                    hiveBoolStats.setNumNulls(booleanColStat.getNullCount());
                }
                return ColumnStatisticsData.booleanStats(hiveBoolStats);
            }
        } else if (type.equals(LogicalTypeRoot.TINYINT)
                || type.equals(LogicalTypeRoot.SMALLINT)
                || type.equals(LogicalTypeRoot.INTEGER)
                || type.equals(LogicalTypeRoot.BIGINT)
                || type.equals(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                || type.equals(LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE)
                || type.equals(LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE)) {
            if (colStat instanceof CatalogColumnStatisticsDataLong) {
                CatalogColumnStatisticsDataLong longColStat =
                        (CatalogColumnStatisticsDataLong) colStat;
                LongColumnStatsData hiveLongColStats = new LongColumnStatsData();
                hiveLongColStats.clear();
                if (null != longColStat.getMax()) {
                    hiveLongColStats.setHighValue(longColStat.getMax());
                }
                if (null != longColStat.getMin()) {
                    hiveLongColStats.setLowValue(longColStat.getMin());
                }
                if (null != longColStat.getNdv()) {
                    hiveLongColStats.setNumDVs(longColStat.getNdv());
                }
                if (null != longColStat.getNullCount()) {
                    hiveLongColStats.setNumNulls(longColStat.getNullCount());
                }
                return ColumnStatisticsData.longStats(hiveLongColStats);
            }
        } else if (type.equals(LogicalTypeRoot.FLOAT) || type.equals(LogicalTypeRoot.DOUBLE)) {
            if (colStat instanceof CatalogColumnStatisticsDataDouble) {
                CatalogColumnStatisticsDataDouble doubleColumnStatsData =
                        (CatalogColumnStatisticsDataDouble) colStat;
                DoubleColumnStatsData hiveFloatStats = new DoubleColumnStatsData();
                hiveFloatStats.clear();
                if (null != doubleColumnStatsData.getMax()) {
                    hiveFloatStats.setHighValue(doubleColumnStatsData.getMax());
                }
                if (null != doubleColumnStatsData.getMin()) {
                    hiveFloatStats.setLowValue(doubleColumnStatsData.getMin());
                }
                if (null != doubleColumnStatsData.getNullCount()) {
                    hiveFloatStats.setNumNulls(doubleColumnStatsData.getNullCount());
                }
                if (null != doubleColumnStatsData.getNdv()) {
                    hiveFloatStats.setNumDVs(doubleColumnStatsData.getNdv());
                }
                return ColumnStatisticsData.doubleStats(hiveFloatStats);
            }
        } else if (type.equals(LogicalTypeRoot.DATE)) {
            if (colStat instanceof CatalogColumnStatisticsDataDate) {
                HiveShim hiveShim = HiveShimLoader.loadHiveShim(hiveVersion);
                return hiveShim.toHiveDateColStats((CatalogColumnStatisticsDataDate) colStat);
            }
        } else if (type.equals(LogicalTypeRoot.VARBINARY) || type.equals(LogicalTypeRoot.BINARY)) {
            if (colStat instanceof CatalogColumnStatisticsDataBinary) {
                CatalogColumnStatisticsDataBinary binaryColumnStatsData =
                        (CatalogColumnStatisticsDataBinary) colStat;
                BinaryColumnStatsData hiveBinaryColumnStats = new BinaryColumnStatsData();
                hiveBinaryColumnStats.clear();
                if (null != binaryColumnStatsData.getMaxLength()) {
                    hiveBinaryColumnStats.setMaxColLen(binaryColumnStatsData.getMaxLength());
                }
                if (null != binaryColumnStatsData.getAvgLength()) {
                    hiveBinaryColumnStats.setAvgColLen(binaryColumnStatsData.getAvgLength());
                }
                if (null != binaryColumnStatsData.getNullCount()) {
                    hiveBinaryColumnStats.setNumNulls(binaryColumnStatsData.getNullCount());
                }
                return ColumnStatisticsData.binaryStats(hiveBinaryColumnStats);
            }
        } else if (type.equals(LogicalTypeRoot.DECIMAL)) {
            if (colStat instanceof CatalogColumnStatisticsDataDouble) {
                CatalogColumnStatisticsDataDouble flinkStats =
                        (CatalogColumnStatisticsDataDouble) colStat;
                DecimalColumnStatsData hiveStats = new DecimalColumnStatsData();
                if (flinkStats.getMax() != null) {
                    // in older versions we cannot create HiveDecimal from Double, so convert Double
                    // to BigDecimal first
                    hiveStats.setHighValue(
                            toThriftDecimal(
                                    HiveDecimal.create(BigDecimal.valueOf(flinkStats.getMax()))));
                }
                if (flinkStats.getMin() != null) {
                    hiveStats.setLowValue(
                            toThriftDecimal(
                                    HiveDecimal.create(BigDecimal.valueOf(flinkStats.getMin()))));
                }
                if (flinkStats.getNdv() != null) {
                    hiveStats.setNumDVs(flinkStats.getNdv());
                }
                if (flinkStats.getNullCount() != null) {
                    hiveStats.setNumNulls(flinkStats.getNullCount());
                }
                return ColumnStatisticsData.decimalStats(hiveStats);
            }
        }
        throw new CatalogException(
                String.format(
                        "Flink does not support converting ColumnStats '%s' for Hive column "
                                + "type '%s' yet",
                        colStat, colType));
    }

    private static Decimal toThriftDecimal(HiveDecimal hiveDecimal) {
        // the constructor signature changed in 3.x. use default constructor and set each field...
        Decimal res = new Decimal();
        res.setUnscaled(ByteBuffer.wrap(hiveDecimal.unscaledValue().toByteArray()));
        res.setScale((short) hiveDecimal.scale());
        return res;
    }

    private static HiveDecimal toHiveDecimal(Decimal decimal) {
        return HiveDecimal.create(new BigInteger(decimal.getUnscaled()), decimal.getScale());
    }

    public static long parsePositiveLongStat(Map<String, String> parameters, String key) {
        String value = parameters.get(key);
        if (value == null) {
            return DEFAULT_UNKNOWN_STATS_VALUE;
        } else {
            long v = Long.parseLong(value);
            return v > 0 ? v : DEFAULT_UNKNOWN_STATS_VALUE;
        }
    }

    // Utilities for merge statistic
    private static Double average(Double a, Long ac, Double b, Long bc) {
        if (a == null || ac == null) {
            return b;
        }
        if (b == null || bc == null) {
            return a;
        }
        return ac + bc == 0 ? null : (a * ac + b * bc) / (ac + bc);
    }

    private static Long min(Long a, Long b) {
        if (a == null || b == null) {
            return a == null ? b : a;
        }

        return Math.min(a, b);
    }

    private static Double min(Double a, Double b) {
        if (a == null || b == null) {
            return a == null ? b : a;
        }

        return Math.min(a, b);
    }

    private static Date min(Date a, Date b) {
        if (a == null || b == null) {
            return a == null ? b : a;
        }

        return a.getDaysSinceEpoch() <= b.getDaysSinceEpoch() ? a : b;
    }

    private static Long max(Long a, Long b) {
        if (a == null || b == null) {
            return a == null ? b : a;
        }

        return Math.max(a, b);
    }

    private static Double max(Double a, Double b) {
        if (a == null || b == null) {
            return a == null ? b : a;
        }

        return Math.max(a, b);
    }

    private static Date max(Date a, Date b) {
        if (a == null || b == null) {
            return a == null ? b : a;
        }

        return a.getDaysSinceEpoch() >= b.getDaysSinceEpoch() ? a : b;
    }

    private static Long add(Long a, Long b) {
        if (a == null || b == null) {
            return a == null ? b : a;
        }

        return a + b;
    }
}
