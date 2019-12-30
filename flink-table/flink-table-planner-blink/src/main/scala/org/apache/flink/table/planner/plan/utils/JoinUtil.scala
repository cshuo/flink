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

package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, FunctionCodeGenerator}
import org.apache.flink.table.planner.utils.TableConfigUtils.isOperatorDisabled
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition
import org.apache.flink.table.types.logical.LogicalType
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.{RelNode, RelVisitor}
import org.apache.calcite.rel.core.{Join, JoinInfo, TableScan}
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.calcite.util.{ImmutableIntList, Util}
import org.apache.calcite.util.mapping.IntPair

import scala.collection.mutable
import java.util

import org.apache.calcite.plan.volcano.RelSubset
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase

/**
  * Util for [[Join]]s.
  */
object JoinUtil {

  /**
    * Check and get join left and right keys.
    */
  def checkAndGetJoinKeys(
      keyPairs: List[IntPair],
      left: RelNode,
      right: RelNode,
      allowEmptyKey: Boolean = false): (Array[Int], Array[Int]) = {
    // get the equality keys
    val leftKeys = mutable.ArrayBuffer.empty[Int]
    val rightKeys = mutable.ArrayBuffer.empty[Int]
    if (keyPairs.isEmpty) {
      if (allowEmptyKey) {
        (leftKeys.toArray, rightKeys.toArray)
      } else {
        throw new TableException(
          s"Joins should have at least one equality condition.\n" +
            s"\tleft: ${left.toString}\n\tright: ${right.toString}\n" +
            s"please re-check the join statement and make sure there's " +
            "equality condition for join.")
      }
    } else {
      // at least one equality expression
      val leftFields = left.getRowType.getFieldList
      val rightFields = right.getRowType.getFieldList

      keyPairs.foreach { pair =>
        val leftKeyType = leftFields.get(pair.source).getType.getSqlTypeName
        val rightKeyType = rightFields.get(pair.target).getType.getSqlTypeName

        // check if keys are compatible
        if (leftKeyType == rightKeyType) {
          // add key pair
          leftKeys += pair.source
          rightKeys += pair.target
        } else {
          throw new TableException(
            s"Join: Equality join predicate on incompatible types. " +
              s"\tLeft: ${left.toString}\n\tright: ${right.toString}\n" +
              "please re-check the join statement.")
        }
      }
      (leftKeys.toArray, rightKeys.toArray)
    }
  }

  /**
    * Creates a [[JoinInfo]] by analyzing a condition.
    *
    * <p>NOTES: the functionality of the method is same with [[JoinInfo#of]],
    * the only difference is that the methods could return `filterNulls`.
    */
  def createJoinInfo(
      left: RelNode,
      right: RelNode,
      condition: RexNode,
      filterNulls: util.List[java.lang.Boolean]): JoinInfo = {
    val leftKeys = new util.ArrayList[Integer]
    val rightKeys = new util.ArrayList[Integer]
    val remaining = RelOptUtil.splitJoinCondition(
      left, right, condition, leftKeys, rightKeys, filterNulls)

    if (remaining.isAlwaysTrue) {
      JoinInfo.of(ImmutableIntList.copyOf(leftKeys), ImmutableIntList.copyOf(rightKeys))
    } else {
      // TODO create NonEquiJoinInfo directly
      JoinInfo.of(left, right, condition)
    }
  }

  def generateConditionFunction(
      config: TableConfig,
      rexBuilder: RexBuilder,
      joinInfo: JoinInfo,
      leftType: LogicalType,
      rightType: LogicalType): GeneratedJoinCondition = {
    val ctx = CodeGeneratorContext(config)
    // should consider null fields
    val exprGenerator = new ExprCodeGenerator(ctx, false)
      .bindInput(leftType)
      .bindSecondInput(rightType)

    val body = if (joinInfo.isEqui) {
      // only equality condition
      "return true;"
    } else {
      val nonEquiPredicates = joinInfo.getRemaining(rexBuilder)
      val condition = exprGenerator.generateExpression(nonEquiPredicates)
      s"""
         |${condition.code}
         |return ${condition.resultTerm};
         |""".stripMargin
    }

    FunctionCodeGenerator.generateJoinCondition(
      ctx,
      "ConditionFunction",
      body)
  }

  def isJoinTypeApplicable(
      join: Join,
      operatorType: OperatorType,
      tableConfig: TableConfig): Boolean = {
    val joinInfo = join.analyzeCondition
    val isJoinTypeEnabled = !isOperatorDisabled(tableConfig, operatorType)
    if (operatorType == OperatorType.NestedLoopJoin) {
      return isJoinTypeEnabled
    }
    !joinInfo.pairs().isEmpty && isJoinTypeEnabled
  }

  /**
   * Get tables names of the left and right input TableScan of a Join.
   */
  def getJoinTableNames(join: Join): (Option[String], Option[String]) = {
    val relVisitor = new RelVisitor {
      var endVisiting = false
      var tableName: Option[String] = Option.empty

      override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
        node match {
          case r: RelSubset => visit(r.getBest, ordinal, parent)
          case _ =>
            if (ordinal > 0) {
              endVisiting = true
            }
            if (endVisiting) {
              return
            }
            if (node.isInstanceOf[TableScan]) {
              tableName = Option(Util.last(
                node.getTable.asInstanceOf[FlinkPreparingTableBase].getNames))
            }
            super.visit(node, ordinal, parent)
        }
      }

      def reset(): Unit = {
        endVisiting = false
        tableName = Option.empty
      }
    }
    relVisitor.go(join.getLeft)
    val leftName = relVisitor.tableName

    relVisitor.reset()
    relVisitor.go(join.getRight)
    val rightName = relVisitor.tableName
    (leftName, rightName)
  }
}
