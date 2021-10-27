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

package org.apache.flink.table.storage.connector;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.FileSystemConnectorOptions;
import org.apache.flink.table.storage.logstore.LogStoreFactory.LogSinkProvider;
import org.apache.flink.table.storage.runtime.RowWriter;
import org.apache.flink.table.storage.runtime.sink.BucketStreamPartitioner;
import org.apache.flink.table.storage.runtime.sink.DynamicSink;
import org.apache.flink.table.storage.runtime.sink.PartitionSelector;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;

/** TODO implement overwrite. */
public class TableStorageSink implements DynamicTableSink, SupportsPartitioning {

    private final TableContext tableContext;
    @Nullable private final LogSinkProvider logSinkProvider;

    private boolean overwrite;
    private LinkedHashMap<String, String> staticPartition;

    public TableStorageSink(TableContext tableContext, @Nullable LogSinkProvider logSinkProvider) {
        this.tableContext = tableContext;
        this.logSinkProvider = logSinkProvider;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context sinkContext) {
        RowWriter rowWriter = tableContext.processor().createRowWriter(tableContext.numBucket());
        DynamicSink sink =
                new DynamicSink(
                        tableContext.table(),
                        tableContext.storeFactory(),
                        new PartitionSelector(
                                tableContext.rowType(),
                                tableContext.partitionKeys(),
                                FileSystemConnectorOptions.PARTITION_DEFAULT_NAME.defaultValue()),
                        rowWriter,
                        tableContext.processor().keySerializer(),
                        logSinkProvider);

        return (DataStreamSinkProvider)
                dataStream -> {
                    // Using StreamPartitioner instead of KeySelector to produce fixed bucket-task
                    // mapping.
                    StreamPartitioner<RowData> partitioner = new BucketStreamPartitioner(rowWriter);
                    Transformation<RowData> transformation =
                            new PartitionTransformation<>(
                                    dataStream.getTransformation(), partitioner);
                    return new DataStream<>(dataStream.getExecutionEnvironment(), transformation)
                            .sinkTo(sink);
                };
    }

    @Override
    public DynamicTableSink copy() {
        TableStorageSink sink = new TableStorageSink(tableContext, logSinkProvider);
        sink.overwrite = overwrite;
        sink.staticPartition = staticPartition;
        return sink;
    }

    @Override
    public String asSummaryString() {
        return "dynamic";
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        this.staticPartition = new LinkedHashMap<>(partition);
    }
}
