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

package org.apache.flink.table.planner.plan.metadata

import org.apache.flink.table.planner.plan.metadata.FlinkMetadata.StatisticsAvailable
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase

import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core._
import org.apache.calcite.rel.metadata._
import org.apache.calcite.rel.rules.MultiJoin

import java.lang.{Boolean => JBoolean}

import scala.collection.JavaConversions._

class FlinkRelMdStatisticsAvailable private extends MetadataHandler[StatisticsAvailable] {
  override def getDef: MetadataDef[StatisticsAvailable] = StatisticsAvailable.DEF

  def getIsStatisticsAvailable(rel: TableScan, mq: RelMetadataQuery): JBoolean = {
    rel.getTable match {
      case table: FlinkPreparingTableBase =>
        table.haveStatistic
      case _ => false
    }
  }

  def getIsStatisticsAvailable(rel: Project, mq: RelMetadataQuery): JBoolean =
    isStatisticsAvailable(rel.getInput, mq)

  def getIsStatisticsAvailable(rel: MultiJoin, mq: RelMetadataQuery): JBoolean = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val properties = rel.getInputs.map(fmq.getIsStatisticsAvailable)
    if (properties.contains(false)) {
      return false
    }
    true
  }

  def getIsStatisticsAvailable(rel: Exchange, mq: RelMetadataQuery): JBoolean =
    isStatisticsAvailable(rel.getInput, mq)

  def getIsStatisticsAvailable(rel: Calc, mq: RelMetadataQuery): JBoolean =
    isStatisticsAvailable(rel.getInput, mq)

  def getIsStatisticsAvailable(rel: Filter, mq: RelMetadataQuery): JBoolean =
    isStatisticsAvailable(rel.getInput, mq)

  def getIsStatisticsAvailable(rel: Sort, mq: RelMetadataQuery): JBoolean =
    isStatisticsAvailable(rel.getInput, mq)

  def getIsStatisticsAvailable(rel: Aggregate, mq: RelMetadataQuery): JBoolean =
    isStatisticsAvailable(rel.getInput, mq)

  def getIsStatisticsAvailable(rel: Window, mq: RelMetadataQuery): JBoolean = {
    isStatisticsAvailable(rel.getInput, mq)
  }

  def getIsStatisticsAvailable(rel: Values, mq: RelMetadataQuery): JBoolean = true

  def getIsStatisticsAvailable(rel: Correlate, mq: RelMetadataQuery): JBoolean = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val properties = rel.getInputs.map(fmq.getIsStatisticsAvailable)
    if (properties.contains(true)) {
      return true
    }
    false
  }

  def getIsStatisticsAvailable(rel: HepRelVertex, mq: RelMetadataQuery): JBoolean = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getIsStatisticsAvailable(rel.getCurrentRel)
  }

  def getIsStatisticsAvailable(rel: Join, mq: RelMetadataQuery): JBoolean = {
    val leftChild = rel.getLeft
    val rightChild = rel.getRight
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getIsStatisticsAvailable(leftChild) && fmq.getIsStatisticsAvailable(rightChild)
  }

  def getIsStatisticsAvailable(rel: Union, mq: RelMetadataQuery): JBoolean = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    val properties = rel.getInputs.map(fmq.getIsStatisticsAvailable)
    if (properties.contains(false)) {
      return false
    }
    true
  }

  def getIsStatisticsAvailable(rel: RelSubset, mq: RelMetadataQuery): JBoolean = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getIsStatisticsAvailable(rel.getBest)
  }

  def getIsStatisticsAvailable(rel: RelNode, mq: RelMetadataQuery): JBoolean = {
    false
  }

  def isStatisticsAvailable(rel: RelNode, mq: RelMetadataQuery): JBoolean = {
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(mq)
    fmq.getIsStatisticsAvailable(rel)
  }
}

object FlinkRelMdStatisticsAvailable {
  private val INSTANCE = new FlinkRelMdStatisticsAvailable

  val SOURCE: RelMetadataProvider = ReflectiveRelMetadataProvider.reflectiveSource(
    FlinkMetadata.StatisticsAvailable.METHOD,
    INSTANCE)
}
