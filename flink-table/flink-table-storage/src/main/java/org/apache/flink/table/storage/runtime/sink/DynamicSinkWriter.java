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

package org.apache.flink.table.storage.runtime.sink;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.filestore.Table;
import org.apache.flink.table.storage.filestore.filter.InFilter;
import org.apache.flink.table.storage.filestore.lsm.FileStore;
import org.apache.flink.table.storage.filestore.lsm.sst.SstFileMeta;
import org.apache.flink.table.storage.filestore.manifest.ManifestEntry;
import org.apache.flink.table.storage.filestore.operation.Scan;
import org.apache.flink.table.storage.logstore.LogStoreFactory.OffsetsRetriever;
import org.apache.flink.table.storage.runtime.RowWriter;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** */
public class DynamicSinkWriter<LogCommT, LogStateT>
        implements SinkWriter<RowData, DynamicCommittable<LogCommT>, LogStateT> {

    private final int task;
    private final Table table;
    private final FileStore.Factory factory;
    private final PartitionSelector partitionSelector;
    private final RowWriter rowWriter;
    @Nullable private final SinkWriter<RowData, LogCommT, LogStateT> logWriter;
    @Nullable private final OffsetsRetriever offsetsRetriever;

    private final Map<String, Map<Integer, FileStore>> stores;

    public DynamicSinkWriter(
            int task,
            Table table,
            FileStore.Factory factory,
            PartitionSelector partitionSelector,
            RowWriter rowWriter,
            @Nullable SinkWriter<RowData, LogCommT, LogStateT> logWriter,
            @Nullable OffsetsRetriever offsetsRetriever) {
        this.task = task;
        this.table = table;
        this.factory = factory;
        this.partitionSelector = partitionSelector;
        this.rowWriter = rowWriter;
        this.logWriter = logWriter;
        this.offsetsRetriever = offsetsRetriever;
        this.stores = new HashMap<>();
    }

    private FileStore createStore(String partition, int bucket) {
        Scan scan = table.newScan().withBucket(bucket);
        if (partition != null) {
            scan = scan.withPartitionFilter(InFilter.equalPartition(partition));
        }
        List<SstFileMeta> sstFiles =
                scan.plan().files.stream().map(ManifestEntry::file).collect(Collectors.toList());
        return factory.create(partition, bucket, sstFiles);
    }

    private FileStore getStore(String partition, int bucket) {
        return stores.computeIfAbsent(partition, p -> new HashMap<>())
                .computeIfAbsent(bucket, b -> createStore(partition, b));
    }

    @Override
    public void write(RowData element, Context context) throws IOException, InterruptedException {
        rowWriter.start(element);
        String partition = partitionSelector.select(element);
        int bucket = rowWriter.selectBucket();
        FileStore store = getStore(partition, bucket);

        switch (element.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                rowWriter.add(store);
                break;
            case UPDATE_BEFORE:
            case DELETE:
                rowWriter.delete(store);
                break;
        }

        if (logWriter != null) {
            logWriter.write(new BucketRowData(rowWriter.selectBucket(), element), context);
        }
    }

    @Override
    public List<DynamicCommittable<LogCommT>> prepareCommit(boolean flush)
            throws IOException, InterruptedException {
        List<DynamicCommittable<LogCommT>> committables = new ArrayList<>();
        for (String partition : stores.keySet()) {
            stores.computeIfPresent(
                    partition,
                    (p, pStores) -> {
                        for (Integer bucket : pStores.keySet()) {
                            pStores.computeIfPresent(
                                    bucket,
                                    (b, s) -> {
                                        DynamicCommittable<LogCommT> committable =
                                                snapshot(p, b, s);
                                        if (committable != null) {
                                            committables.add(committable);
                                            return s;
                                        }
                                        return null;
                                    });
                        }

                        return pStores.isEmpty() ? null : pStores;
                    });
        }

        if (logWriter != null) {
            Preconditions.checkNotNull(offsetsRetriever);
            List<LogCommT> logCommittables = logWriter.prepareCommit(flush);

            // Compute full buckets by task id
            int[] buckets = BucketStreamPartitioner.taskBuckets(task, rowWriter.numBucket());
            Map<Integer, Long> offsets = offsetsRetriever.endOffsets(buckets);
            committables.add(new DynamicCommittable<>(logCommittables, offsets));
        }

        return committables;
    }

    private DynamicCommittable<LogCommT> snapshot(String partition, int bucket, FileStore store) {
        List<SstFileMeta> addFiles = new ArrayList<>();
        List<SstFileMeta> deleteFiles = new ArrayList<>();
        store.snapshot(addFiles, deleteFiles);
        if (addFiles.isEmpty() && deleteFiles.isEmpty()) {
            return null;
        }

        return new DynamicCommittable<>(
                partition, bucket, rowWriter.numBucket(), addFiles, deleteFiles);
    }

    @Override
    public List<LogStateT> snapshotState(long checkpointId) throws IOException {
        if (logWriter != null) {
            return logWriter.snapshotState(checkpointId);
        }
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        if (logWriter != null) {
            logWriter.close();
        }
        if (offsetsRetriever != null) {
            this.offsetsRetriever.close();
        }
    }
}
