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
import org.apache.flink.table.storage.file.Table;
import org.apache.flink.table.storage.file.lsm.FileStore;
import org.apache.flink.table.storage.file.lsm.StoreException;
import org.apache.flink.table.storage.file.lsm.sst.SstFileMeta;
import org.apache.flink.table.storage.runtime.RowWriter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** */
public class DynamicSinkWriter implements SinkWriter<RowData, DynamicCommittable, Long> {

    private final Table table;
    private final FileStore.Factory factory;
    private final PartitionSelector partitionSelector;
    private final RowWriter rowWriter;
    @Nullable private final SinkWriter<RowData, ?, ?> kafkaWriter;

    private final Map<String, Map<Integer, FileStore>> stores;

    public DynamicSinkWriter(
            Table table,
            FileStore.Factory factory,
            PartitionSelector partitionSelector,
            RowWriter rowWriter,
            @Nullable SinkWriter<RowData, ?, ?> kafkaWriter) {
        this.table = table;
        this.factory = factory;
        this.partitionSelector = partitionSelector;
        this.rowWriter = rowWriter;
        this.kafkaWriter = kafkaWriter;
        this.stores = new HashMap<>();
    }

    private FileStore createStore(String partition, int bucket) {
        Table.PartitionFiles files;
        try {
            files = table.readLatest(partition);
        } catch (IOException e) {
            throw new StoreException(e);
        }
        List<SstFileMeta> sstFiles = files.bucketFiles(bucket);
        return factory.create(
                partition, bucket, sstFiles == null ? Collections.emptyList() : sstFiles);
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

        if (kafkaWriter != null) {
            kafkaWriter.write(element, context);
        }
    }

    @Override
    public List<DynamicCommittable> prepareCommit(boolean flush) {
        List<DynamicCommittable> committables = new ArrayList<>();
        for (String partition : stores.keySet()) {
            stores.computeIfPresent(
                    partition,
                    (p, pStores) -> {
                        for (Integer bucket : pStores.keySet()) {
                            pStores.computeIfPresent(
                                    bucket,
                                    (b, s) -> {
                                        DynamicCommittable committable = snapshot(p, b, s);
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

        return committables;
    }

    private DynamicCommittable snapshot(String partition, int bucket, FileStore store) {
        List<SstFileMeta> addFiles = new ArrayList<>();
        List<SstFileMeta> deleteFiles = new ArrayList<>();
        store.snapshot(addFiles, deleteFiles);
        if (addFiles.isEmpty() && deleteFiles.isEmpty()) {
            return null;
        }

        return new DynamicCommittable(
                partition, bucket, rowWriter.numBucket(), addFiles, deleteFiles);
    }

    @Override
    public List<Long> snapshotState() throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        if (kafkaWriter != null) {
            kafkaWriter.close();
        }
    }
}
