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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.filestore.Table;
import org.apache.flink.table.storage.filestore.manifest.ManifestEntry;
import org.apache.flink.table.storage.runtime.source.DynamicSource;
import org.apache.flink.table.utils.PartitionPathUtils;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** */
public class TableStorageSource implements ScanTableSource, SupportsPartitionPushDown {

    private final TableContext tableContext;

    private final Long snapshotId;

    @Nullable private final DynamicTableSource logTableSource;

    private List<Map<String, String>> remainingPartitions;

    public TableStorageSource(
            TableContext tableContext,
            Long snapshotId,
            @Nullable DynamicTableSource logTableSource) {
        this.tableContext = tableContext;
        this.snapshotId = snapshotId;
        this.logTableSource = logTableSource;
    }

    @Override
    public DynamicTableSource copy() {
        TableStorageSource source =
                new TableStorageSource(tableContext, snapshotId, logTableSource);
        source.remainingPartitions = remainingPartitions;
        return source;
    }

    @Override
    public String asSummaryString() {
        return "dynamic";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return tableContext.runtimeExecutionMode() == RuntimeExecutionMode.STREAMING
                ? ChangelogMode.all()
                : ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext sourceContext) {
        if (tableContext.runtimeExecutionMode() == RuntimeExecutionMode.STREAMING) {
            if (logTableSource == null) {
                throw new TableException("Log table source is null in streaming mode!");
            }

            if (snapshotId == null) {
                return ((ScanTableSource) logTableSource).getScanRuntimeProvider(sourceContext);
            }

            // TODO to source provider after new source watermark push down
            return new DataStreamScanProvider() {
                @Override
                public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
                    Source logSource =
                            ((SourceTransformation)
                                            ((DataStreamScanProvider)
                                                            ((ScanTableSource) logTableSource)
                                                                    .getScanRuntimeProvider(
                                                                            sourceContext))
                                                    .produceDataStream(execEnv)
                                                    .getTransformation())
                                    .getSource();

                    // TODO use SourceFactory to infer snapshot lazied
                    return execEnv.fromSource(
                            HybridSource.builder(createFileSource()).addSource(logSource).build(),
                            WatermarkStrategy.noWatermarks(),
                            "HybridSource-" + tableContext.identifier());
                }

                @Override
                public boolean isBounded() {
                    return false;
                }
            };
        } else {
            return SourceProvider.of(createFileSource());
        }
    }

    private DynamicSource createFileSource() {
        List<String> partitions =
                tableContext.partitionKeys().isEmpty() ? null : getOrFetchPartitions();

        return new DynamicSource(
                tableContext.table(),
                snapshotId,
                tableContext.storeFactory(),
                tableContext.processor().createRowReader(),
                tableContext.processor().keySerializer(),
                partitions);
    }

    /** TODO remove {@link SupportsPartitionPushDown} and use {@link SupportsFilterPushDown}. */
    @Override
    public Optional<List<Map<String, String>>> listPartitions() {
        try {
            Table table = tableContext.table();
            if (table.path().getFileSystem().exists(table.path())) {
                return Optional.of(
                        table.newScan().plan().stream()
                                .map(ManifestEntry::partition)
                                .distinct()
                                .map(Path::new)
                                .map(PartitionPathUtils::extractPartitionSpecFromPath)
                                .collect(Collectors.toList()));
            } else {
                return Optional.of(Collections.emptyList());
            }
        } catch (Exception e) {
            throw new TableException("Fetch partitions fail.", e);
        }
    }

    @Override
    public void applyPartitions(List<Map<String, String>> remainingPartitions) {
        this.remainingPartitions = remainingPartitions;
    }

    private List<String> getOrFetchPartitions() {
        if (remainingPartitions == null) {
            remainingPartitions = listPartitions().get();
        }
        return remainingPartitions.stream()
                .map(p -> (LinkedHashMap<String, String>) p)
                .map(PartitionPathUtils::generatePartitionPath)
                .collect(Collectors.toList());
    }
}
