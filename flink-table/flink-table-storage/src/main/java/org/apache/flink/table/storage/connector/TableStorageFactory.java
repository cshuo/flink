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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DefaultDynamicTableFactory;
import org.apache.flink.table.factories.DefaultLogTableFactory;
import org.apache.flink.table.factories.DefaultLogTableFactory.OffsetsRetrieverFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.storage.logstore.LogStoreFactory;
import org.apache.flink.table.storage.logstore.LogStoreFactory.LogSinkProvider;
import org.apache.flink.table.storage.logstore.LogStoreFactory.LogSourceProvider;
import org.apache.flink.table.storage.logstore.LogStoreFactoryContextImpl;
import org.apache.flink.table.storage.logstore.kafka.KafkaLogStoreFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.table.storage.Options.BUCKET;
import static org.apache.flink.table.storage.Options.CHANGE_TRACKING;
import static org.apache.flink.table.storage.Options.FILE_FORMAT;
import static org.apache.flink.table.storage.Options.FILE_META_FORMAT;
import static org.apache.flink.table.storage.Options.FILE_META_TARGET_FILE_SIZE;
import static org.apache.flink.table.storage.Options.FILE_ROOT_PATH;
import static org.apache.flink.table.storage.Options.FILE_TARGET_FILE_SIZE;
import static org.apache.flink.table.storage.Options.SNAPSHOTS_NUM_RETAINED;
import static org.apache.flink.table.storage.Options.TABLE_STORAGE_PREFIX;
import static org.apache.flink.util.Preconditions.checkArgument;

/** */
public class TableStorageFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory, DefaultDynamicTableFactory {

    public static final String LOG_OPTION_PREFIX = "log.";

    private final LogStoreFactory logStoreFactory = new KafkaLogStoreFactory();

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        LogSinkProvider logSinkProvider = null;
        Map<String, String> options = context.getCatalogTable().getOptions();
        if (changeTracking(options)) {
            logSinkProvider = logStoreFactory.getSinkProvider(createLogStoreContext(context));
        }

        return new TableStorageSink(new TableContext(this, context), logSinkProvider);
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        LogSourceProvider logSourceProvider = null;
        TableContext tableContext = new TableContext(this, context);
        if (tableContext.isStreamExecution()) {
            checkArgument(
                    changeTracking(context.getCatalogTable().getOptions()),
                    "Table must enable change tracking in streaming mode.");
            logSourceProvider = logStoreFactory.getSourceProvider(createLogStoreContext(context));
        }
        return new TableStorageSource(tableContext, logSourceProvider);
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
            logStoreFactory.onTableCreation(createLogStoreContext(context, newOptions));
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
            logStoreFactory.onTableDrop(createLogStoreContext(context));
        }
    }

    private Optional<OffsetsRetrieverFactory> createOffsetsRetrieverFactory(Context context) {
        Map<String, String> tableOptions = context.getCatalogTable().getOptions();
        if (changeTracking(tableOptions)) {
            DefaultLogTableFactory logTableFactory =
                    DefaultDynamicTableFactory.discoverDefaultLogFactory(context.getClassLoader());
            return Optional.of(
                    logTableFactory.createOffsetsRetrieverFactory(
                            logContext(context, tableOptions)));
        }
        return Optional.empty();
    }

    private Context logContext(Context context, Map<String, String> options) {
        return new FactoryUtil.DefaultDynamicTableContext(
                context.getObjectIdentifier(),
                context.getCatalogTable().copy(logOptions(options)),
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

    private LogStoreFactory.Context createLogStoreContext(Context context) {
        return createLogStoreContext(context, null);
    }

    private LogStoreFactory.Context createLogStoreContext(
            Context context, Map<String, String> options) {
        if (options == null) {
            options = context.getCatalogTable().getOptions();
        }

        return new LogStoreFactoryContextImpl(
                context.getObjectIdentifier(),
                context.getCatalogTable().getResolvedSchema(),
                logOptions(options),
                context.getConfiguration(),
                context.getClassLoader(),
                context.isTemporary(),
                bucket(options));
    }

    private int bucket(Map<String, String> options) {
        return Integer.parseInt(
                options.getOrDefault(BUCKET.key(), BUCKET.defaultValue().toString()));
    }
}
