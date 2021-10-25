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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DefaultDynamicTableFactory;
import org.apache.flink.table.factories.DefaultLogTableFactory;
import org.apache.flink.table.factories.DefaultLogTableFactory.OffsetsRetrieverFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.table.storage.TableStorageOptions.BUCKET;
import static org.apache.flink.table.storage.TableStorageOptions.CHANGE_TRACKING;
import static org.apache.flink.table.storage.TableStorageOptions.FILE_FORMAT;
import static org.apache.flink.table.storage.TableStorageOptions.FILE_META_FORMAT;
import static org.apache.flink.table.storage.TableStorageOptions.FILE_META_TARGET_FILE_SIZE;
import static org.apache.flink.table.storage.TableStorageOptions.FILE_ROOT_PATH;
import static org.apache.flink.table.storage.TableStorageOptions.FILE_TARGET_FILE_SIZE;
import static org.apache.flink.table.storage.TableStorageOptions.SNAPSHOTS_NUM_RETAINED;
import static org.apache.flink.table.storage.TableStorageOptions.TABLE_STORAGE_PREFIX;

/** */
public class TableStorageFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory, DefaultDynamicTableFactory {

    public static final String LOG_OPTION_PREFIX = "log.";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        DynamicTableSink logSink =
                createLogTable(context)
                        .map(
                                catalogTable ->
                                        FactoryUtil.createTableSink(
                                                null,
                                                context.getObjectIdentifier(),
                                                catalogTable,
                                                context.getConfiguration(),
                                                context.getClassLoader(),
                                                context.isTemporary()))
                        .orElse(null);
        OffsetsRetrieverFactory offsetsRetrieverFactory =
                createOffsetsRetrieverFactory(context).orElse(null);
        return new TableStorageSink(this, context, logSink, offsetsRetrieverFactory);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        DynamicTableSource logSource =
                createLogTable(context)
                        .map(
                                catalogTable ->
                                        FactoryUtil.createTableSource(
                                                null,
                                                context.getObjectIdentifier(),
                                                catalogTable,
                                                context.getConfiguration(),
                                                context.getClassLoader(),
                                                context.isTemporary()))
                        .orElse(null);
        return new TableStorageSource(this, context, logSource);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CHANGE_TRACKING);
        options.add(FILE_ROOT_PATH);
        options.add(FILE_FORMAT);
        options.add(FILE_META_FORMAT);
        options.add(BUCKET);
        options.add(FILE_META_TARGET_FILE_SIZE);
        options.add(FILE_TARGET_FILE_SIZE);
        options.add(SNAPSHOTS_NUM_RETAINED);
        return options;
    }

    @Override
    public Map<String, String> onTableCreation(Context context) {
        CatalogTable table = context.getCatalogTable();
        Map<String, String> newOptions = new HashMap<>(table.getOptions());
        ((Configuration) context.getConfiguration())
                .toMap()
                .forEach(
                        (k, v) -> {
                            if (k.startsWith(TABLE_STORAGE_PREFIX)) {
                                newOptions.putIfAbsent(
                                        k.substring(TABLE_STORAGE_PREFIX.length()), v);
                            }
                        });

        Path path = tablePath(newOptions, context.getObjectIdentifier());
        try {
            path.getFileSystem().mkdirs(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (changeTracking(newOptions)) {
            DefaultLogTableFactory<?> logFactory =
                    DefaultDynamicTableFactory.discoverDefaultLogFactory(context.getClassLoader());
            int numBucket =
                    Integer.parseInt(
                            newOptions.getOrDefault(
                                    BUCKET.key(), BUCKET.defaultValue().toString()));
            Map<String, String> newLogOptions =
                    logFactory.onTableCreation(copyContext(context, newOptions), numBucket);
            newLogOptions.forEach((k, v) -> newOptions.put(LOG_OPTION_PREFIX + k, v));
        }

        return newOptions;
    }

    @Override
    public void onTableDrop(Context context) {
        Map<String, String> options = context.getCatalogTable().getOptions();
        Path path = tablePath(options, context.getObjectIdentifier());
        try {
            path.getFileSystem().delete(path, true);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (changeTracking(options)) {
            DefaultLogTableFactory<?> logFactory =
                    DefaultDynamicTableFactory.discoverDefaultLogFactory(context.getClassLoader());
            logFactory.onTableDrop(copyContext(context, logOptions(options)));
        }
    }

    private Optional<ResolvedCatalogTable> createLogTable(Context context) {
        Map<String, String> tableOptions = context.getCatalogTable().getOptions();
        if (changeTracking(tableOptions)) {
            Map<String, String> logOptions = logOptions(tableOptions);
            return Optional.of(
                    new ResolvedCatalogTable(
                            context.getCatalogTable().getOrigin().copy(logOptions),
                            context.getCatalogTable().getResolvedSchema()));
        }
        return Optional.empty();
    }

    private Optional<OffsetsRetrieverFactory> createOffsetsRetrieverFactory(Context context) {
        Map<String, String> tableOptions = context.getCatalogTable().getOptions();
        if (changeTracking(tableOptions)) {
            Map<String, String> logOptions = logOptions(tableOptions);
            DefaultLogTableFactory<Serializable> logTableFactory =
                    DefaultDynamicTableFactory.discoverDefaultLogFactory(context.getClassLoader());
            return Optional.of(
                    logTableFactory.createOffsetsRetrieverFactory(
                            copyContext(context, logOptions(tableOptions))));
        }
        return Optional.empty();
    }

    private Context copyContext(Context context, Map<String, String> newOptions) {
        return new FactoryUtil.DefaultDynamicTableContext(
                context.getObjectIdentifier(),
                context.getCatalogTable().copy(newOptions),
                context.getConfiguration(),
                context.getClassLoader(),
                context.isTemporary());
    }

    static boolean changeTracking(Map<String, String> options) {
        return Boolean.parseBoolean(
                options.getOrDefault(
                        CHANGE_TRACKING.key(), CHANGE_TRACKING.defaultValue().toString()));
    }

    static Path tablePath(Map<String, String> options, ObjectIdentifier identifier) {
        return new Path(new Path(options.get(FILE_ROOT_PATH.key())), identifier.asSummaryString());
    }

    /** @return log options from table options. */
    static Map<String, String> logOptions(Map<String, String> tableOptions) {
        Map<String, String> options = new HashMap<>();
        tableOptions.forEach(
                (k, v) -> {
                    if (k.startsWith(LOG_OPTION_PREFIX)) {
                        options.put(k.substring(LOG_OPTION_PREFIX.length()), v);
                    }
                });
        return options;
    }
}
