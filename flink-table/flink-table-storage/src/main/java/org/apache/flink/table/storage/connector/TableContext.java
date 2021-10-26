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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.storage.Options;
import org.apache.flink.table.storage.filestore.Table;
import org.apache.flink.table.storage.filestore.TableImpl;
import org.apache.flink.table.storage.filestore.lsm.FileStore;
import org.apache.flink.table.storage.filestore.lsm.StoreOptions;
import org.apache.flink.table.storage.filestore.manifest.ManifestFileReader;
import org.apache.flink.table.storage.filestore.manifest.ManifestFileWriter;
import org.apache.flink.table.storage.filestore.manifest.ManifestsFileReader;
import org.apache.flink.table.storage.filestore.manifest.ManifestsFileWriter;
import org.apache.flink.table.storage.filestore.utils.FileFactory;
import org.apache.flink.table.storage.runtime.Processor;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.UUID;

import static org.apache.flink.table.storage.Options.BUCKET;
import static org.apache.flink.table.storage.Options.FILE_COMMIT_FORCE_COMPACT;
import static org.apache.flink.table.storage.Options.FILE_COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT;
import static org.apache.flink.table.storage.Options.FILE_COMPACTION_SIZE_RATIO;
import static org.apache.flink.table.storage.Options.FILE_FORMAT;
import static org.apache.flink.table.storage.Options.FILE_LEVEL0_NUM_FILES;
import static org.apache.flink.table.storage.Options.FILE_META_FORMAT;
import static org.apache.flink.table.storage.Options.FILE_META_TARGET_FILE_SIZE;
import static org.apache.flink.table.storage.Options.FILE_MIN_FILE_SIZE;
import static org.apache.flink.table.storage.Options.FILE_NUM_LEVELS;
import static org.apache.flink.table.storage.Options.FILE_TARGET_FILE_SIZE;
import static org.apache.flink.table.storage.Options.LOG_SCAN_STARTUP_MODE;
import static org.apache.flink.table.storage.Options.SNAPSHOTS_NUM_EXPIRE_TRIGGER;
import static org.apache.flink.table.storage.Options.SNAPSHOTS_NUM_RETAINED;
import static org.apache.flink.table.storage.connector.TableStorageFactory.tablePath;

/** */
public class TableContext {

    private final TableStorageFactory factory;
    private final DynamicTableFactory.Context context;
    private final Processor processor;
    private final RowType rowType;
    private final List<String> partitionKeys;
    private final int numBucket;
    private final Path tablePath;
    private final Configuration options;

    private Table table;
    private FileStore.Factory storeFactory;

    public TableContext(TableStorageFactory factory, DynamicTableFactory.Context context) {
        this.factory = factory;
        this.context = context;
        this.options = new Configuration();
        context.getCatalogTable().getOptions().forEach(this.options::setString);

        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        this.rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();
        this.tablePath =
                tablePath(context.getCatalogTable().getOptions(), context.getObjectIdentifier());
        this.partitionKeys = context.getCatalogTable().getPartitionKeys();
        this.numBucket = options.get(BUCKET);
        this.processor = Processor.create(schema);
    }

    public Table table() {
        if (table == null) {
            initialize();
        }

        return table;
    }

    public FileStore.Factory storeFactory() {
        if (storeFactory == null) {
            initialize();
        }

        return storeFactory;
    }

    private void initialize() {
        TableStorageFormats formats =
                new TableStorageFormats(
                        factory, context, processor.keyType(), processor.valueType());
        String uuid = UUID.randomUUID().toString();

        FileFactory manifestName =
                new FileFactory(
                        new Path(tablePath, "manifest"),
                        "manifest",
                        uuid,
                        options.get(FILE_META_FORMAT));

        FileFactory manifestsName =
                new FileFactory(
                        new Path(tablePath, "manifest"),
                        "manifests",
                        uuid,
                        options.get(FILE_META_FORMAT));

        FileFactory snapshotName =
                new FileFactory(new Path(tablePath, "snapshot"), "snapshot", uuid, "json");

        table =
                new TableImpl(
                        options.get(SNAPSHOTS_NUM_EXPIRE_TRIGGER),
                        options.get(SNAPSHOTS_NUM_RETAINED),
                        options.get(FILE_META_TARGET_FILE_SIZE).getBytes(),
                        tablePath,
                        new ManifestFileWriter(formats.getManifestWriter(), manifestName),
                        new ManifestFileReader(
                                formats.getManifestReader(),
                                processor.keySerializer().getArity(),
                                processor.keySerializer()),
                        new ManifestsFileWriter(formats.getManifestsWriter(), manifestsName),
                        new ManifestsFileReader(formats.getManifestsReader()),
                        snapshotName);

        storeFactory =
                new FileStoreFactory(
                        new StoreOptions(
                                options.get(FILE_LEVEL0_NUM_FILES),
                                options.get(FILE_NUM_LEVELS),
                                options.get(FILE_TARGET_FILE_SIZE).getBytes(),
                                options.get(FILE_MIN_FILE_SIZE).getBytes(),
                                options.get(FILE_COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT),
                                options.get(FILE_COMPACTION_SIZE_RATIO),
                                options.get(FILE_COMMIT_FORCE_COMPACT)),
                        processor.comparator(),
                        tablePath,
                        processor.keySerializer(),
                        processor.valueSerializer(),
                        processor.compactStrategy(),
                        formats,
                        options.get(FILE_FORMAT));
    }

    public ObjectIdentifier identifier() {
        return context.getObjectIdentifier();
    }

    public Processor processor() {
        return processor;
    }

    public Options.LogScanStartupMode logScanStartupMode() {
        return options.get(LOG_SCAN_STARTUP_MODE);
    }

    public int numBucket() {
        return numBucket;
    }

    public RowType rowType() {
        return rowType;
    }

    public List<String> partitionKeys() {
        return partitionKeys;
    }

    public boolean isStreamExecution() {
        return context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE)
                == RuntimeExecutionMode.STREAMING;
    }
}
