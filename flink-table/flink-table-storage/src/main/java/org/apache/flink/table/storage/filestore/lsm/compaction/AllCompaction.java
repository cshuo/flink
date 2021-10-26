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

package org.apache.flink.table.storage.filestore.lsm.compaction;

import org.apache.flink.table.storage.filestore.lsm.Level;

import java.util.List;

/** */
public class AllCompaction implements CompactStrategy {

    public static final AllCompaction INSTANCE = new AllCompaction();

    @Override
    public CompactionUnit pick(List<Level> levels) {
        return new CompactionUnit(levels.size() - 1, createRuns(levels));
    }
}
