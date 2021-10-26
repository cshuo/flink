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

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.BulkReaderFormatFactory;
import org.apache.flink.table.factories.BulkWriterFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.storage.filestore.lsm.sst.SstFileWriter;
import org.apache.flink.table.storage.filestore.manifest.ManifestEntry;
import org.apache.flink.table.storage.filestore.manifest.ManifestFileMeta;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;
import static org.apache.flink.table.storage.TableStorageOptions.FILE_FORMAT;
import static org.apache.flink.table.storage.TableStorageOptions.FILE_META_FORMAT;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/** */
public class TableStorageFormats implements Serializable {

    private final BulkWriter.Factory<RowData> dataWriter;
    private final BulkFormat<RowData, FileSourceSplit> dataReader;

    private final BulkWriter.Factory<RowData> manifestWriter;
    private final BulkFormat<RowData, FileSourceSplit> manifestReader;

    private final BulkWriter.Factory<RowData> manifestsWriter;
    private final BulkFormat<RowData, FileSourceSplit> manifestsReader;

    public TableStorageFormats(
            TableStorageFactory factory,
            DynamicTableFactory.Context context,
            RowType keyType,
            RowType valueType) {
        RowType dataType = SstFileWriter.schema(keyType, valueType);
        DynamicTableFactory.Context dataContext = newContext(context, dataType);

        DynamicTableSink.Context sinkContext = createSinkContext();
        DynamicTableSource.Context sourceContext = createSourceContext();
        this.dataWriter =
                createTableFactoryHelper(factory, dataContext)
                        .discoverEncodingFormat(BulkWriterFormatFactory.class, FILE_FORMAT)
                        .createRuntimeEncoder(sinkContext, fromLogicalToDataType(dataType));
        this.dataReader =
                createTableFactoryHelper(factory, dataContext)
                        .discoverDecodingFormat(BulkReaderFormatFactory.class, FILE_FORMAT)
                        .createRuntimeDecoder(sourceContext, fromLogicalToDataType(dataType));

        RowType manifestType = ManifestEntry.schema(keyType);
        DynamicTableFactory.Context manifestContext = newContext(context, manifestType);
        this.manifestWriter =
                createTableFactoryHelper(factory, manifestContext)
                        .discoverEncodingFormat(BulkWriterFormatFactory.class, FILE_META_FORMAT)
                        .createRuntimeEncoder(sinkContext, fromLogicalToDataType(manifestType));
        this.manifestReader =
                createTableFactoryHelper(factory, manifestContext)
                        .discoverDecodingFormat(BulkReaderFormatFactory.class, FILE_META_FORMAT)
                        .createRuntimeDecoder(sourceContext, fromLogicalToDataType(manifestType));

        RowType manifestsType = ManifestFileMeta.schema();
        DynamicTableFactory.Context snapshotContext = newContext(context, manifestsType);
        this.manifestsWriter =
                createTableFactoryHelper(factory, snapshotContext)
                        .discoverEncodingFormat(BulkWriterFormatFactory.class, FILE_META_FORMAT)
                        .createRuntimeEncoder(sinkContext, fromLogicalToDataType(manifestsType));
        this.manifestsReader =
                createTableFactoryHelper(factory, snapshotContext)
                        .discoverDecodingFormat(BulkReaderFormatFactory.class, FILE_META_FORMAT)
                        .createRuntimeDecoder(sourceContext, fromLogicalToDataType(manifestsType));
    }

    private DynamicTableFactory.Context newContext(
            DynamicTableFactory.Context context, RowType type) {
        ResolvedCatalogTable oldTable = context.getCatalogTable();
        List<Column.PhysicalColumn> columns = new ArrayList<>();
        for (RowType.RowField field : type.getFields()) {
            Column.PhysicalColumn physical =
                    Column.physical(field.getName(), fromLogicalToDataType(field.getType()));
            columns.add(physical);
        }
        ResolvedSchema schema = ResolvedSchema.of(columns.toArray(new Column[0]));
        CatalogTable newTable =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(schema).build(),
                        oldTable.getComment(),
                        oldTable.getPartitionKeys(),
                        oldTable.getOptions());
        return new FactoryUtil.DefaultDynamicTableContext(
                context.getObjectIdentifier(),
                new ResolvedCatalogTable(newTable, schema),
                context.getConfiguration(),
                context.getClassLoader(),
                context.isTemporary());
    }

    private DynamicTableSink.Context createSinkContext() {
        return new DynamicTableSink.Context() {

            @Override
            public boolean isBounded() {
                return false;
            }

            @Override
            public <T> TypeInformation<T> createTypeInformation(DataType consumedDataType) {
                return InternalTypeInfo.of(consumedDataType.getLogicalType());
            }

            @Override
            public <T> TypeInformation<T> createTypeInformation(LogicalType consumedLogicalType) {
                return InternalTypeInfo.of(consumedLogicalType);
            }

            @Override
            public DynamicTableSink.DataStructureConverter createDataStructureConverter(
                    DataType consumedDataType) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private DynamicTableSource.Context createSourceContext() {
        return new DynamicTableSource.Context() {

            @Override
            public <T> TypeInformation<T> createTypeInformation(DataType consumedDataType) {
                return InternalTypeInfo.of(consumedDataType.getLogicalType());
            }

            @Override
            public <T> TypeInformation<T> createTypeInformation(LogicalType producedLogicalType) {
                return InternalTypeInfo.of(producedLogicalType);
            }

            @Override
            public DynamicTableSource.DataStructureConverter createDataStructureConverter(
                    DataType consumedDataType) {
                throw new UnsupportedOperationException();
            }
        };
    }

    public BulkWriter.Factory<RowData> getDataWriter() {
        return dataWriter;
    }

    public BulkFormat<RowData, FileSourceSplit> getDataReader() {
        return dataReader;
    }

    public BulkWriter.Factory<RowData> getManifestWriter() {
        return manifestWriter;
    }

    public BulkFormat<RowData, FileSourceSplit> getManifestReader() {
        return manifestReader;
    }

    public BulkWriter.Factory<RowData> getManifestsWriter() {
        return manifestsWriter;
    }

    public BulkFormat<RowData, FileSourceSplit> getManifestsReader() {
        return manifestsReader;
    }
}
