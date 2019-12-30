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

package org.apache.flink.table.planner.plan.batch.sql.join

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.utils.{BatchTableTestUtil, TableTestBase}
import org.junit.Test

class HintedJoinTest extends TableTestBase {

  protected val util: BatchTableTestUtil = batchTestUtil()
  util.addTableSource[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)
  util.addTableSource[(Int, Long, Int, String, Long)]("MyTable2", 'd, 'e, 'f, 'g, 'h)
  util.addTableSource[(Int, Long, String)]("MyTable3", 'x, 'y, 'z)


  @Test
  def testHintedBroadcastJoin(): Unit = {
    util.verifyPlan("SELECT /*+ USE_BROADCAST(MyTable1) */ c, g\n"
      + "FROM MyTable2, MyTable1 WHERE a = d AND d < 2")
  }

  @Test
  def testHintedShuffleHashJoin(): Unit = {
    util.verifyPlan("SELECT /*+ USE_HASH(MyTable1) */ c, g\n"
      + "FROM MyTable2, MyTable1 WHERE a = d AND d < 2")
  }

  @Test
  def testHintedSortMergeJoin(): Unit = {
    util.verifyPlan("SELECT /*+ USE_MERGE(MyTable1, MyTable2) */ c, g\n"
      + "FROM MyTable2, MyTable1 WHERE a = d AND d < 2")
  }

  @Test
  def testHintedNestedLoopJoin(): Unit = {
    util.verifyPlan("SELECT /*+ USE_NL(MyTable1) */ c, g\n"
      + "FROM MyTable2, MyTable1 WHERE a = d AND d < 2")
  }

  @Test
  def testMultiHintsConflict(): Unit = {
    util.verifyPlan("SELECT /*+ USE_NL(MyTable1), USE_MERGE(MyTable2), USE_BROADCAST(MyTable1) */\n"
      + "c, g FROM MyTable2, MyTable1 WHERE a = d AND d < 2")
  }

  @Test
  def testHintedMultiJoins(): Unit = {
    util.verifyPlan("SELECT /*+  use_broadcast(MyTable1), use_nl(MyTable3) */\n"
      + "c, g, z FROM MyTable2, MyTable1, MyTable3 WHERE a = x AND a = d AND d < 2")
  }

  @Test
  def testNegativeJoinHints(): Unit = {
    util.verifyPlan("SELECT /*+  use_broadcast(MyTable1), no_use_hash */\n"
      + "c, g, z FROM MyTable2, MyTable1, MyTable3 WHERE a = x AND a = d AND d < 2")
  }
}
