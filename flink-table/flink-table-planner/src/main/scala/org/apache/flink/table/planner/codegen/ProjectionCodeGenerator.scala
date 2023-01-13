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
package org.apache.flink.table.planner.codegen

import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.binary.BinaryRowData
import org.apache.flink.table.data.writer.BinaryRowWriter
import org.apache.flink.table.planner.codegen.CodeGenUtils._
import org.apache.flink.table.planner.codegen.GeneratedExpression.{NEVER_NULL, NO_CODE}
import org.apache.flink.table.planner.codegen.GenerateUtils.generateRecordStatement
import org.apache.flink.table.planner.functions.aggfunctions._
import org.apache.flink.table.planner.plan.utils.AggregateInfo
import org.apache.flink.table.runtime.generated.{GeneratedProjection, Projection}
import org.apache.flink.table.types.logical.RowType

import scala.collection.mutable.ArrayBuffer

/**
 * CodeGenerator for projection, Take out some fields of [[RowData]] to generate a new [[RowData]].
 */
object ProjectionCodeGenerator {

  def generateProjectionExpression(
      ctx: CodeGeneratorContext,
      inType: RowType,
      outType: RowType,
      inputMapping: Array[Int],
      outClass: Class[_ <: RowData] = classOf[BinaryRowData],
      inputTerm: String = DEFAULT_INPUT1_TERM,
      outRecordTerm: String = DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = DEFAULT_OUT_RECORD_WRITER_TERM,
      reusedOutRecord: Boolean = true): GeneratedExpression = {
    val exprGenerator = new ExprCodeGenerator(ctx, false)
      .bindInput(inType, inputTerm = inputTerm, inputFieldMapping = Option(inputMapping))
    val accessExprs =
      inputMapping.map(idx => GenerateUtils.generateFieldAccess(ctx, inType, inputTerm, idx))
    val expression = exprGenerator.generateResultExpression(
      accessExprs,
      outType,
      outClass,
      outRow = outRecordTerm,
      outRowWriter = Option(outRecordWriterTerm),
      reusedOutRow = reusedOutRecord)

    val outRowInitCode = {
      val initCode =
        generateRecordStatement(outType, outClass, outRecordTerm, Some(outRecordWriterTerm), ctx)
      if (reusedOutRecord) {
        NO_CODE
      } else {
        initCode
      }
    }

    val code =
      s"""
         |$outRowInitCode
         |${expression.code}
        """.stripMargin
    GeneratedExpression(outRecordTerm, NEVER_NULL, code, outType)
  }

  /**
   * CodeGenerator for projection.
   * @param reusedOutRecord
   *   If objects or variables can be reused, they will be added a reusable output record to the
   *   member area of the generated class. If not they will be as temp variables.
   * @return
   */
  def generateProjection(
      ctx: CodeGeneratorContext,
      name: String,
      inType: RowType,
      outType: RowType,
      inputMapping: Array[Int],
      outClass: Class[_ <: RowData] = classOf[BinaryRowData],
      inputTerm: String = DEFAULT_INPUT1_TERM,
      outRecordTerm: String = DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = DEFAULT_OUT_RECORD_WRITER_TERM,
      reusedOutRecord: Boolean = true): GeneratedProjection = {
    val className = newName(name)
    val baseClass = classOf[Projection[_, _]]

    val expression = generateProjectionExpression(
      ctx,
      inType,
      outType,
      inputMapping,
      outClass,
      inputTerm,
      outRecordTerm,
      outRecordWriterTerm,
      reusedOutRecord)

    val code =
      s"""
         |public class $className implements ${baseClass.getCanonicalName}<$ROW_DATA, ${outClass.getCanonicalName}> {
         |
         |  ${ctx.reuseMemberCode()}
         |
         |  public $className(Object[] references) throws Exception {
         |    ${ctx.reuseInitCode()}
         |  }
         |
         |  @Override
         |  public ${outClass.getCanonicalName} apply($ROW_DATA $inputTerm) {
         |    ${ctx.reuseLocalVariableCode()}
         |    ${expression.code}
         |    return ${expression.resultTerm};
         |  }
         |}
        """.stripMargin

    new GeneratedProjection(className, code, ctx.references.toArray, ctx.tableConfig)
  }

  /**
   * If adaptive hash agg takes effect, and the sample result was to suppress local hash agg. In
   * order to ensure that the data format transmitted downstream with doing local hash agg is
   * consistent with the data format transmitted downstream without doing local hash agg, we need to
   * do projection for grouping function value.
   *
   * <p> For example, for sql statement "select a, avg(b), count(c) from T group by a", if local
   * hash agg suppressed and a row (1, 5, "a") comes to local hash agg, we will pass (1, 5, 1, 1) to
   * downstream.
   */
  def generatedAdaptiveHashAggValueProjectionCode(
      ctx: CodeGeneratorContext,
      inputType: RowType,
      outClass: Class[_ <: RowData] = classOf[BinaryRowData],
      inputTerm: String = DEFAULT_INPUT1_TERM,
      aggInfos: Array[AggregateInfo],
      outRecordTerm: String = DEFAULT_OUT_RECORD_TERM,
      outRecordWriterTerm: String = DEFAULT_OUT_RECORD_WRITER_TERM): String = {
    val fieldExprs: ArrayBuffer[GeneratedExpression] = ArrayBuffer()
    aggInfos.map {
      aggInfo =>
        aggInfo.function match {
          case _: SumAggFunction =>
            fieldExprs += GenerateUtils.generateFieldAccess(
              ctx,
              inputType,
              inputTerm,
              aggInfo.agg.getArgList.get(0))
          case _: MaxAggFunction | _: MinAggFunction =>
            fieldExprs += GenerateUtils.generateFieldAccess(
              ctx,
              inputType,
              inputTerm,
              aggInfo.agg.getArgList.get(0))
          case _: AvgAggFunction =>
            fieldExprs += GenerateUtils.generateFieldAccess(
              ctx,
              inputType,
              inputTerm,
              aggInfo.agg.getArgList.get(0))
            fieldExprs += GenerateUtils.generateFieldAccessForCountSpecificCol(
              ctx,
              inputTerm,
              aggInfo.agg.getArgList.get(0))
          case _: CountAggFunction =>
            fieldExprs += GenerateUtils.generateFieldAccessForCountSpecificCol(
              ctx,
              inputTerm,
              aggInfo.agg.getArgList.get(0))
          case _: Count1AggFunction =>
            fieldExprs += GenerateUtils.generateFieldAccessForCountOne(ctx)
        }
    }

    val binaryRowWriter = CodeGenUtils.className[BinaryRowWriter]
    val typeTerm = outClass.getCanonicalName
    ctx.addReusableMember(s"$typeTerm $outRecordTerm= new $typeTerm(${fieldExprs.size});")
    ctx.addReusableMember(
      s"$binaryRowWriter $outRecordWriterTerm = new $binaryRowWriter($outRecordTerm);")

    val fieldExprIdxToOutputRowPosMap = fieldExprs.indices.map(i => i -> i).toMap
    val setFieldsCode = fieldExprs.zipWithIndex
      .map {
        case (fieldExpr, index) =>
          val pos = fieldExprIdxToOutputRowPosMap.getOrElse(
            index,
            throw new CodeGenException(s"Illegal field expr index: $index"))
          rowSetField(
            ctx,
            classOf[BinaryRowData],
            outRecordTerm,
            pos.toString,
            fieldExpr,
            Option(outRecordWriterTerm))
      }
      .mkString("\n")

    val writer = outRecordWriterTerm
    val resetWriter = s"$writer.reset();"
    val completeWriter: String = s"$writer.complete();"
    s"""
       |$resetWriter
       |$setFieldsCode
       |$completeWriter
        """.stripMargin
  }

  /** For java invoke. */
  def generateProjection(
      ctx: CodeGeneratorContext,
      name: String,
      inputType: RowType,
      outputType: RowType,
      inputMapping: Array[Int]): GeneratedProjection =
    generateProjection(
      ctx,
      name,
      inputType,
      outputType,
      inputMapping,
      inputTerm = DEFAULT_INPUT1_TERM)

  /** For java invoke. */
  def generateProjection(
      ctx: CodeGeneratorContext,
      name: String,
      inputType: RowType,
      outputType: RowType,
      inputMapping: Array[Int],
      outClass: Class[_ <: RowData]): GeneratedProjection =
    generateProjection(
      ctx,
      name,
      inputType,
      outputType,
      inputMapping,
      outClass = outClass,
      inputTerm = DEFAULT_INPUT1_TERM)
}
