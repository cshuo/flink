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
import org.apache.flink.table.storage.filestore.lsm.sst.SstFileMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** */
public interface CompactStrategy {

    default List<CompactionUnit.SortedRun> createRuns(List<Level> levels) {
        List<CompactionUnit.SortedRun> runs = new ArrayList<>();

        for (SstFileMeta file : levels.get(0).files()) {
            runs.add(
                    new CompactionUnit.SortedRun(
                            0, file.getFileSize(), Collections.singletonList(file)));
        }

        for (int i = 1; i < levels.size(); i++) {
            Level level = levels.get(i);
            if (level.files().size() > 0) {
                runs.add(new CompactionUnit.SortedRun(i, level.totalSize(), level.files()));
            }
        }

        return runs;
    }

    CompactionUnit pick(List<Level> levels);
}
