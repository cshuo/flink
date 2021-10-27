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
import org.apache.flink.table.storage.filestore.Table;
import org.apache.flink.table.storage.filestore.lsm.FileStore;
import org.apache.flink.table.storage.logstore.LogStoreFactory.LogSinkProvider;
import org.apache.flink.table.storage.logstore.LogStoreFactory.OffsetsRetriever;
import org.apache.flink.table.storage.runtime.RowWriter;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** */
public class DynamicSink<LogCommT, LogStateT>
        implements Sink<
                RowData, DynamicCommittable<LogCommT>, LogStateT, DynamicCommittable<LogCommT>> {

    private final Table table;
    private final FileStore.Factory factory;
    private final PartitionSelector partitionSelector;
    private final RowWriter rowWriter;
    private final RowDataSerializer keySerializer;

    @Nullable private final Sink<RowData, LogCommT, LogStateT, ?> logSink;

    @Nullable private final LogSinkProvider logSinkProvider;

    public DynamicSink(
            Table table,
            FileStore.Factory factory,
            PartitionSelector partitionSelector,
            RowWriter rowWriter,
            RowDataSerializer keySerializer,
            @Nullable LogSinkProvider logSinkProvider) {
        this.table = table;
        this.factory = factory;
        this.partitionSelector = partitionSelector;
        this.keySerializer = keySerializer;
        this.rowWriter = rowWriter;
        this.logSink = logSinkProvider == null ? null : (Sink) logSinkProvider.createSink();
        this.logSinkProvider = logSinkProvider;
    }

    @Override
    public DynamicSinkWriter<LogCommT, LogStateT> createWriter(
            InitContext context, List<LogStateT> states) throws IOException {
        SinkWriter<RowData, LogCommT, LogStateT> kafkaWriter =
                logSink == null ? null : logSink.createWriter(context, states);
        OffsetsRetriever offsetsRetriever =
                logSinkProvider == null ? null : logSinkProvider.createOffsetsRetriever();
        return new DynamicSinkWriter<>(
                context.getSubtaskId(),
                table,
                factory,
                partitionSelector,
                rowWriter,
                kafkaWriter,
                offsetsRetriever);
    }

    @Override
    public Optional<SimpleVersionedSerializer<LogStateT>> getWriterStateSerializer() {
        return logSink == null ? Optional.empty() : logSink.getWriterStateSerializer();
    }

    @Override
    public Optional<Committer<DynamicCommittable<LogCommT>>> createCommitter() throws IOException {
        LocalCommitter<LogCommT> localCommitter = null;
        if (logSink != null) {
            Optional<Committer<LogCommT>> committer = logSink.createCommitter();
            if (committer.isPresent()) {
                localCommitter = new LocalCommitter<>(committer.get());
            }
        }
        return Optional.ofNullable(localCommitter);
    }

    @Override
    public Optional<SimpleVersionedSerializer<DynamicCommittable<LogCommT>>>
            getCommittableSerializer() {
        SimpleVersionedSerializer<LogCommT> logSerializer =
                Optional.ofNullable(logSink)
                        .map(Sink::getCommittableSerializer)
                        .flatMap(o -> o)
                        .orElse(null);
        return Optional.of(new DynamicCommittableSerializer<>(keySerializer, logSerializer));
    }

    @Override
    public Optional<GlobalCommitter<DynamicCommittable<LogCommT>, DynamicCommittable<LogCommT>>>
            createGlobalCommitter() throws IOException {
        if (logSink != null) {
            if (logSink.createGlobalCommitter().isPresent()) {
                throw new UnsupportedOperationException("Unsupported yet.");
            }
        }
        return Optional.of(new DynamicGlobalCommitter<>(table));
    }

    @Override
    public Optional<SimpleVersionedSerializer<DynamicCommittable<LogCommT>>>
            getGlobalCommittableSerializer() {
        return getCommittableSerializer();
    }
}
