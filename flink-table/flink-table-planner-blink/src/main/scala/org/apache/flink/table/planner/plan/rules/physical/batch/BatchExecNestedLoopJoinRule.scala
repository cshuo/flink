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
package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.table.planner.calcite.FlinkContext
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecNestedLoopJoin
import org.apache.flink.table.planner.plan.utils.{HintUtils, JoinUtil, OperatorType}
import org.apache.flink.table.planner.utils.TableConfigUtils.isOperatorDisabled
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.planner.plan.hints.Hints.JoinHintType

/**
  * Rule that converts [[FlinkLogicalJoin]] to [[BatchExecNestedLoopJoin]]
  * if NestedLoopJoin is enabled.
  */
class BatchExecNestedLoopJoinRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin],
      operand(classOf[RelNode], any)),
    "BatchExecNestedLoopJoinRule")
  with BatchExecJoinRuleBase
  with BatchExecNestedLoopJoinRuleBase {

  override def matches(call: RelOptRuleCall): Boolean = {
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    val join: Join = call.rel(0)

    val hintJoinType = HintUtils.getApplicableJoinHintType(join, tableConfig)
    if (hintJoinType.isPresent) {
      if (hintJoinType.get() == JoinHintType.NNLJ) {
        return false
      } else if (!HintUtils.isNegativeJoinHint(hintJoinType.get())) {
        return hintJoinType.get() == JoinHintType.NLJ
      }
    }

    !isOperatorDisabled(tableConfig, OperatorType.NestedLoopJoin)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: Join = call.rel(0)
    val left = join.getLeft
    val right = join.getJoinType match {
      case JoinRelType.SEMI | JoinRelType.ANTI =>
        // We can do a distinct to buildSide(right) when semi join.
        val distinctKeys = 0 until join.getRight.getRowType.getFieldCount
        val useBuildDistinct = chooseSemiBuildDistinct(join.getRight, distinctKeys)
        if (useBuildDistinct) {
          addLocalDistinctAgg(join.getRight, distinctKeys, call.builder())
        } else {
          join.getRight
        }
      case _ => join.getRight
    }
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    val leftIsBuild = isLeftBuild(join, left, right, tableConfig)
    val newJoin = createNestedLoopJoin(join, left, right, leftIsBuild, singleRowJoin = false)
    call.transformTo(newJoin)
  }

  private def isLeftBuild(
      join: Join, left: RelNode, right: RelNode, tableConfig: TableConfig): Boolean = {
    // check the join hint.
    val hintJoinType = HintUtils.getApplicableJoinHintType(join, tableConfig)
    if (hintJoinType.isPresent && hintJoinType.get() == JoinHintType.NLJ) {
      val (left, right) = JoinUtil.getJoinTableNames(join)
      // check table names in the join hint, ignore it if there are more than 1 table names.
      val hintTableNames = HintUtils.getJoinHintContent(
        left, right, join.getHints, JoinHintType.NLJ)
      if (hintTableNames.size() == 1) {
        return left.isDefined && hintTableNames.get(0).equals(left.get)
      }
    }

    join.getJoinType match {
      case JoinRelType.LEFT => false
      case JoinRelType.RIGHT => true
      case JoinRelType.INNER | JoinRelType.FULL =>
        val leftSize = binaryRowRelNodeSize(left)
        val rightSize = binaryRowRelNodeSize(right)
        // use left as build size if leftSize or rightSize is unknown.
        if (leftSize == null || rightSize == null) {
          true
        } else {
          leftSize <= rightSize
        }
      case JoinRelType.SEMI | JoinRelType.ANTI => false
    }
  }
}

object BatchExecNestedLoopJoinRule {
  val INSTANCE: RelOptRule = new BatchExecNestedLoopJoinRule
}
