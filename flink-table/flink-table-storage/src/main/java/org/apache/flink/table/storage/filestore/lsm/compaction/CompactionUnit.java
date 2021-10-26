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

import org.apache.flink.table.storage.filestore.lsm.sst.SstFileMeta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/** */
public class CompactionUnit {

    private final int outputLevel;

    private final List<SortedRun> runs;

    public CompactionUnit(int outputLevel, List<SortedRun> runs) {
        checkArgument(outputLevel >= 0);
        checkArgument(runs.size() > 1);

        this.outputLevel = outputLevel;
        this.runs = Collections.unmodifiableList(runs);
    }

    public int outputLevel() {
        return outputLevel;
    }

    public List<SstFileMeta> files() {
        return runs.stream().flatMap(run -> run.files().stream()).collect(Collectors.toList());
    }

    public List<SortedRun> runs() {
        return runs;
    }

    /** */
    public static class SortedRun {

        private final int level;

        private final long size;

        private final List<SstFileMeta> files;

        public SortedRun(int level, long size, Collection<SstFileMeta> files) {
            this.level = level;
            this.size = size;
            this.files = Collections.unmodifiableList(new ArrayList<>(files));
        }

        public int level() {
            return level;
        }

        public long size() {
            return size;
        }

        public List<SstFileMeta> files() {
            return files;
        }
    }
}
