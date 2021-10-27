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

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.table.storage.filestore.Snapshot;
import org.apache.flink.table.storage.filestore.Table;
import org.apache.flink.table.storage.filestore.filter.InFilter;
import org.apache.flink.table.storage.filestore.operation.Scan;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

/** */
public class DynamicSplitEnumerator implements SplitEnumerator<DynamicSplit, Void> {

    private final SplitEnumeratorContext<DynamicSplit> context;

    private final Queue<DynamicSplit> splits;

    @Nullable private final Snapshot snapshot;

    public DynamicSplitEnumerator(
            SplitEnumeratorContext<DynamicSplit> context,
            Table table,
            Long snapshotId,
            List<String> partitions) {
        this.context = context;
        this.splits = new ArrayDeque<>();
        Scan scan = table.newScan().withSnapshot(snapshotId);
        if (partitions != null) {
            scan = scan.withPartitionFilter(InFilter.inPartition(partitions));
        }
        Scan.Plan plan = scan.plan();
        this.snapshot = plan.snapshot;
        Scan.groupByPartition(plan.files)
                .forEach(
                        (partition, pFiles) ->
                                pFiles.forEach(
                                        (bucket, files) ->
                                                splits.add(
                                                        new DynamicSplit(
                                                                partition, bucket, files))));
    }

    @Nullable
    public Snapshot getSnapshot() {
        return snapshot;
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (splits.isEmpty()) {
            context.signalNoMoreSplits(subtaskId);
        } else {
            context.assignSplit(splits.poll(), subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<DynamicSplit> splits, int subtaskId) {
        this.splits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {}

    @Override
    public Void snapshotState(long checkpointId) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {}
}
