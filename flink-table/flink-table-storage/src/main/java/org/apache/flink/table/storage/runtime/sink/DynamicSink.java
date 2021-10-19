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

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.storage.file.DynamicTable;
import org.apache.flink.table.storage.file.lsm.FileStore;
import org.apache.flink.table.storage.runtime.RowWriter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** */
public class DynamicSink implements Sink<RowData, DynamicCommittable, Long, DynamicCommittable> {

    private final DynamicTable table;
    private final FileStore.Factory factory;
    private final PartitionSelector partitionSelector;
    private final RowWriter rowWriter;
    private final RowDataSerializer keySerializer;

    @Nullable private final Sink<RowData, ?, ?, ?> kafka;

    public DynamicSink(
            DynamicTable table,
            FileStore.Factory factory,
            PartitionSelector partitionSelector,
            RowWriter rowWriter,
            RowDataSerializer keySerializer,
            @Nullable Sink<RowData, ?, ?, ?> kafka) {
        this.table = table;
        this.factory = factory;
        this.partitionSelector = partitionSelector;
        this.keySerializer = keySerializer;
        this.rowWriter = rowWriter;
        this.kafka = kafka;
    }

    @Override
    public SinkWriter<RowData, DynamicCommittable, Long> createWriter(
            InitContext context, List<Long> states) throws IOException {
        SinkWriter<RowData, ?, ?> kafkaWriter =
                kafka == null ? null : kafka.createWriter(context, Collections.emptyList());
        return new DynamicSinkWriter(table, factory, partitionSelector, rowWriter, kafkaWriter);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Long>> getWriterStateSerializer() {
        return Optional.of(new DynamicWriterStateSerializer());
    }

    @Override
    public Optional<Committer<DynamicCommittable>> createCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<DynamicCommittable>> getCommittableSerializer() {
        return Optional.of(new DynamicCommittableSerializer(keySerializer));
    }

    @Override
    public Optional<GlobalCommitter<DynamicCommittable, DynamicCommittable>>
            createGlobalCommitter() {
        return Optional.of(new DynamicGlobalCommitter(table));
    }

    @Override
    public Optional<SimpleVersionedSerializer<DynamicCommittable>>
            getGlobalCommittableSerializer() {
        return getCommittableSerializer();
    }
}
