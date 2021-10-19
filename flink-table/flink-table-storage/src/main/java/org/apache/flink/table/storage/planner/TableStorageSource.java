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

package org.apache.flink.table.storage.planner;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.BuiltInDynamicTableSource;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.storage.file.DynamicTable;
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
public class TableStorageSource extends TableStorageSourceSink
        implements ScanTableSource, BuiltInDynamicTableSource, SupportsPartitionPushDown {

    private final DynamicTableFactory.Context context;

    @Nullable private final DynamicTableSource kafkaSource;

    private List<Map<String, String>> remainingPartitions;

    private RuntimeExecutionMode mode;

    public TableStorageSource(
            TableStorageFactory factory,
            DynamicTableFactory.Context context,
            @Nullable DynamicTableSource kafkaSource) {
        super(factory, context);
        this.context = context;
        this.kafkaSource = kafkaSource;
    }

    @Override
    public DynamicTableSource copy() {
        TableStorageSource source = new TableStorageSource(factory, context, kafkaSource);
        source.remainingPartitions = remainingPartitions;
        source.mode = mode;
        return source;
    }

    @Override
    public String asSummaryString() {
        return "dynamic";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return mode == RuntimeExecutionMode.STREAMING
                ? ChangelogMode.all()
                : ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext sourceContext) {
        if (mode == RuntimeExecutionMode.STREAMING) {
            if (kafkaSource == null) {
                throw new TableException("kafkaSource is null in streaming mode!");
            }

            if (remainingPartitions != null) {
                // TODO how to to?
            }
            return ((ScanTableSource) kafkaSource).getScanRuntimeProvider(sourceContext);
        } else {
            List<String> partitions =
                    context.getCatalogTable().isPartitioned() ? getOrFetchPartitions() : null;

            DynamicSource source =
                    new DynamicSource(
                            table(),
                            storeFactory(),
                            processor.createRowReader(),
                            processor.keySerializer(),
                            partitions);
            return SourceProvider.of(source);
        }
    }

    @Override
    public Optional<List<Map<String, String>>> listPartitions() {
        try {
            DynamicTable table = table();
            if (table.path().getFileSystem().exists(table.path())) {
                return Optional.of(
                        table.allPartitions().stream()
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

    @Override
    public void setRuntimeMode(RuntimeExecutionMode mode) {
        this.mode = mode;
    }
}
