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

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DefaultLogTableFactory.OffsetsRetrieverFactory;
import org.apache.flink.table.filesystem.FileSystemConnectorOptions;
import org.apache.flink.table.storage.runtime.RowWriter;
import org.apache.flink.table.storage.runtime.sink.BucketKeySelector;
import org.apache.flink.table.storage.runtime.sink.DynamicSink;
import org.apache.flink.table.storage.runtime.sink.PartitionSelector;

import javax.annotation.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;

/** */
public class TableStorageSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {

    private final TableContext tableContext;
    @Nullable private final DynamicTableSink logTableSink;
    @Nullable private final OffsetsRetrieverFactory offsetsRetrieverFactory;

    private boolean overwrite;
    private LinkedHashMap<String, String> staticPartition;

    public TableStorageSink(
            TableContext tableContext,
            @Nullable DynamicTableSink logTableSink,
            @Nullable OffsetsRetrieverFactory offsetsRetrieverFactory) {
        this.tableContext = tableContext;
        this.logTableSink = logTableSink;
        this.offsetsRetrieverFactory = offsetsRetrieverFactory;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context sinkContext) {
        Sink<RowData, ?, ?, ?> logSink =
                logTableSink == null
                        ? null
                        : ((SinkProvider) logTableSink.getSinkRuntimeProvider(sinkContext))
                                .createSink();

        if (overwrite) {
            // TODO...
        }

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
                        logSink,
                        offsetsRetrieverFactory);

        return (DataStreamSinkProvider)
                dataStream -> dataStream.keyBy(new BucketKeySelector(rowWriter)).sinkTo(sink);
    }

    @Override
    public DynamicTableSink copy() {
        TableStorageSink sink =
                new TableStorageSink(tableContext, logTableSink, offsetsRetrieverFactory);
        sink.overwrite = overwrite;
        sink.staticPartition = staticPartition;
        return sink;
    }

    @Override
    public String asSummaryString() {
        return "dynamic";
    }

    @Override
    public void applyOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        this.staticPartition = new LinkedHashMap<>(partition);
    }
}
