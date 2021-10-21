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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.storage.filestore.lsm.FileStore;
import org.apache.flink.table.storage.filestore.lsm.FileStoreImpl;
import org.apache.flink.table.storage.filestore.lsm.StoreOptions;
import org.apache.flink.table.storage.filestore.lsm.merge.MergePolicy;
import org.apache.flink.table.storage.filestore.lsm.sst.SstFileMeta;
import org.apache.flink.table.storage.filestore.utils.FileFactory;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/** */
public class FileStoreFactory implements FileStore.Factory {

    private final StoreOptions options;
    private final GeneratedRecordComparator keyComparator;
    private final Path tablePath;
    private final RowType keyType;
    private final RowType valueType;
    private final MergePolicy mergePolicy;
    private final TableStorageFormats formats;
    private final String dataFormat;

    public FileStoreFactory(
            StoreOptions options,
            GeneratedRecordComparator keyComparator,
            Path tablePath,
            RowType keyType,
            RowType valueType,
            MergePolicy mergePolicy,
            TableStorageFormats formats,
            String dataFormat) {
        this.options = options;
        this.mergePolicy = mergePolicy;
        this.keyComparator = keyComparator;
        this.tablePath = tablePath;
        this.keyType = keyType;
        this.valueType = valueType;
        this.formats = formats;
        this.dataFormat = dataFormat;
    }

    @Override
    public FileStore create(String partition, int bucket, List<SstFileMeta> sstFiles) {
        RecordComparator comparator =
                keyComparator.newInstance(Thread.currentThread().getContextClassLoader());
        return new FileStoreImpl(
                options,
                new Path(tablePath, FileFactory.BUCKET_DIR_PREFIX + bucket),
                keyType.getFieldCount(),
                valueType.getFieldCount(),
                keyType,
                valueType,
                comparator,
                formats.getDataWriter(),
                formats.getDataReader(),
                dataFormat,
                mergePolicy,
                sstFiles);
    }
}
