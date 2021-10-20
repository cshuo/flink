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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.storage.file.DynamicTable;
import org.apache.flink.table.storage.file.lsm.FileStore;
import org.apache.flink.table.storage.file.lsm.StoreOptions;
import org.apache.flink.table.storage.file.manifest.ManifestFileReader;
import org.apache.flink.table.storage.file.manifest.ManifestFileWriter;
import org.apache.flink.table.storage.file.snapshot.SnapshotFileReader;
import org.apache.flink.table.storage.file.snapshot.SnapshotFileWriter;
import org.apache.flink.table.storage.file.utils.FileFactory;
import org.apache.flink.table.storage.runtime.Processor;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.UUID;

import static org.apache.flink.table.factories.DefaultDynamicTableFactory.BUCKET;
import static org.apache.flink.table.storage.TableStorageOptions.FILE_COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT;
import static org.apache.flink.table.storage.TableStorageOptions.FILE_COMPACTION_SIZE_RATIO;
import static org.apache.flink.table.storage.TableStorageOptions.FILE_FORMAT;
import static org.apache.flink.table.storage.TableStorageOptions.FILE_LEVEL0_NUM_FILES;
import static org.apache.flink.table.storage.TableStorageOptions.FILE_META_FORMAT;
import static org.apache.flink.table.storage.TableStorageOptions.FILE_META_TARGET_FILE_SIZE;
import static org.apache.flink.table.storage.TableStorageOptions.FILE_MIN_FILE_SIZE;
import static org.apache.flink.table.storage.TableStorageOptions.FILE_NUM_LEVELS;
import static org.apache.flink.table.storage.TableStorageOptions.FILE_TARGET_FILE_SIZE;
import static org.apache.flink.table.storage.TableStorageOptions.SNAPSHOTS_NUM_RETAINED;
import static org.apache.flink.table.storage.planner.TableStorageFactory.tablePath;

/** */
public abstract class TableStorageSourceSink {

    protected final TableStorageFactory factory;
    protected final DynamicTableFactory.Context context;
    protected final Processor processor;
    protected final RowType rowType;
    protected final List<String> partitionKeys;
    protected final int numBucket;
    private final Path tablePath;
    private final Configuration options;

    private DynamicTable table;
    private FileStore.Factory storeFactory;

    public TableStorageSourceSink(
            TableStorageFactory factory, DynamicTableFactory.Context context) {
        this.factory = factory;
        this.context = context;
        this.options = new Configuration();
        context.getCatalogTable().getOptions().forEach(this.options::setString);

        ResolvedSchema schema = context.getCatalogTable().getResolvedSchema();
        this.rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();
        this.tablePath =
                tablePath(context.getCatalogTable().getOptions(), context.getObjectIdentifier());
        this.partitionKeys = context.getCatalogTable().getPartitionKeys();

        List<String> fieldNames = rowType.getFieldNames();
        RowDataKeySelector keySelector =
                schema.getPrimaryKey()
                        .map(k -> k.getColumns().stream().mapToInt(fieldNames::indexOf).toArray())
                        .map(
                                keys ->
                                        KeySelectorUtil.getRowDataSelector(
                                                keys, InternalTypeInfo.of(rowType)))
                        .orElse(null);

        this.numBucket = options.get(BUCKET);

        this.processor =
                Processor.create(
                        keySelector,
                        rowType,
                        type ->
                                ComparatorCodeGenerator.gen(
                                        new TableConfig(),
                                        "KeyComparator",
                                        type,
                                        SortSpec.defaultSortAll(type.getFieldCount())));
    }

    public DynamicTable table() {
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

        FileFactory mainifestName =
                new FileFactory(
                        new Path(tablePath, "manifest"),
                        "manifest",
                        uuid,
                        options.get(FILE_META_FORMAT));
        FileFactory snapshotName =
                new FileFactory(
                        new Path(tablePath, "snapshot"),
                        "snapshot",
                        uuid,
                        options.get(FILE_META_FORMAT));

        table =
                new DynamicTable(
                        options.get(SNAPSHOTS_NUM_RETAINED),
                        options.get(FILE_META_TARGET_FILE_SIZE).getBytes(),
                        tablePath,
                        new ManifestFileWriter(formats.getManifestWriter(), mainifestName),
                        new ManifestFileReader(
                                formats.getManifestReader(),
                                processor.keySerializer().getArity(),
                                processor.keySerializer()),
                        new SnapshotFileWriter(formats.getSnapshotWriter(), snapshotName),
                        new SnapshotFileReader(formats.getSnapshotReader()));

        storeFactory =
                new FileStoreFactory(
                        new StoreOptions(
                                options.get(FILE_LEVEL0_NUM_FILES),
                                options.get(FILE_NUM_LEVELS),
                                options.get(FILE_TARGET_FILE_SIZE).getBytes(),
                                options.get(FILE_MIN_FILE_SIZE).getBytes(),
                                options.get(FILE_COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT),
                                options.get(FILE_COMPACTION_SIZE_RATIO)),
                        processor.comparator(),
                        tablePath,
                        processor.keySerializer(),
                        processor.valueSerializer(),
                        processor.compactStrategy(),
                        formats,
                        options.get(FILE_FORMAT));
    }
}
