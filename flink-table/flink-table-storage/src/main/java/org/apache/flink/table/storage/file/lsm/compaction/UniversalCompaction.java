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

package org.apache.flink.table.storage.file.lsm.compaction;

import org.apache.flink.table.storage.file.lsm.Level;
import org.apache.flink.table.storage.file.lsm.compaction.CompactionUnit.SortedRun;
import org.apache.flink.table.storage.file.lsm.sst.SstFileMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * See RocksDb Universal-Compaction: https://github.com/facebook/rocksdb/wiki/Universal-Compaction.
 */
public class UniversalCompaction implements CompactStrategy {

    private final int maxSizeAmp;
    private final int sizeRatio;
    private final int maxRunNum;

    public UniversalCompaction(int maxSizeAmp, int sizeRatio, int maxRunNum) {
        this.maxSizeAmp = maxSizeAmp;
        this.sizeRatio = sizeRatio;
        this.maxRunNum = maxRunNum;
    }

    @Override
    public CompactionUnit pick(List<Level> levels) {
        // TODO just compact all files if the total size is too smaller than target file size?

        int maxLevel = levels.size() - 1;

        // 1. construct sort runs
        List<SortedRun> runs = new ArrayList<>();

        for (SstFileMeta file : levels.get(0).files()) {
            runs.add(new SortedRun(0, file.getFileSize(), Collections.singletonList(file)));
        }

        for (int i = 1; i < levels.size(); i++) {
            Level level = levels.get(i);
            if (level.files().size() > 0) {
                runs.add(new SortedRun(i, level.totalSize(), level.files()));
            }
        }

        // 2. pick
        if (runs.size() >= maxRunNum) {
            // 2.1 checking for reducing size amplification
            CompactionUnit unit = pickForSizeAmp(maxLevel, runs);
            if (unit != null) {
                return unit;
            }

            // 2.2 checking for size ratio
            unit = pickForSizeRatio(maxLevel, runs);
            if (unit != null) {
                return unit;
            }

            // 2.3 checking for file num
            if (runs.size() > maxRunNum) {
                // compacting for file num
                return createUnit(runs, maxLevel, 0, runs.size() - maxRunNum + 1);
            }
        }

        return null;
    }

    private CompactionUnit pickForSizeAmp(int maxLevel, List<SortedRun> runs) {
        if (runs.size() > 1) {
            long candidateSize =
                    runs.subList(0, runs.size() - 1).stream().mapToLong(SortedRun::size).sum();

            long earliestRunSize = runs.get(runs.size() - 1).size();

            // size amplification = percentage of additional size
            if (candidateSize * 100 >= maxSizeAmp * earliestRunSize) {
                return new CompactionUnit(maxLevel, runs);
            }
        }

        return null;
    }

    private CompactionUnit pickForSizeRatio(int maxLevel, List<SortedRun> runs) {
        for (int i = 0; i < runs.size(); i++) {
            int candidateCount = 1;
            long candidateSize = runs.get(i).size();

            for (int j = i + 1; j < runs.size(); j++) {
                SortedRun next = runs.get(j);
                if (candidateSize * (100.0 + sizeRatio) / 100.0 < next.size()) {
                    break;
                }

                candidateSize += next.size();
                candidateCount++;
            }

            if (candidateCount > 1) {
                return createUnit(runs, maxLevel, i, i + candidateCount);
            }
        }

        return null;
    }

    private CompactionUnit createUnit(
            List<SortedRun> runs, int maxLevel, int fromIndex, int toIndex) {
        int outputLevel;
        if (toIndex == runs.size()) {
            outputLevel = maxLevel;
        } else if (runs.get(toIndex).level() == 0) {
            outputLevel = 0;
        } else {
            outputLevel = runs.get(toIndex).level() - 1;
        }

        return new CompactionUnit(outputLevel, runs.subList(fromIndex, toIndex));
    }
}
