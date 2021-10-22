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

package org.apache.flink.table.storage.runtime.source;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.file.lsm.FileStore;
import org.apache.flink.table.storage.runtime.RowReader;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;

/** */
public class DynamicSplitReader implements SplitReader<RecordAndPosition<RowData>, DynamicSplit> {

    private final FileStore.Factory factory;
    private final RowReader rowReader;
    private final Queue<DynamicSplit> splits;

    public DynamicSplitReader(FileStore.Factory factory, RowReader rowReader) {
        this.factory = factory;
        this.rowReader = rowReader;
        this.splits = new ArrayDeque<>();
    }

    @Override
    public RecordsWithSplitIds<RecordAndPosition<RowData>> fetch() throws IOException {
        DynamicSplit split = splits.poll();
        Preconditions.checkNotNull(split);
        FileStore store =
                factory.create(split.getPartition(), split.getBucketId(), split.getFiles());
        Iterator<RecordAndPosition<RowData>> iterator = rowReader.readRecords(store.iterator());
        return new RecordsWithSplitIds<RecordAndPosition<RowData>>() {

            private String nextSplit = split.splitId();

            @Nullable
            @Override
            public String nextSplit() {
                final String nextSplit = this.nextSplit;
                this.nextSplit = null;
                return nextSplit;
            }

            @Nullable
            @Override
            public RecordAndPosition<RowData> nextRecordFromSplit() {
                if (iterator.hasNext()) {
                    return iterator.next();
                }

                return null;
            }

            @Override
            public Set<String> finishedSplits() {
                return Collections.singleton(split.splitId());
            }
        };
    }

    @Override
    public void handleSplitsChanges(SplitsChange<DynamicSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        splits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {}
}
