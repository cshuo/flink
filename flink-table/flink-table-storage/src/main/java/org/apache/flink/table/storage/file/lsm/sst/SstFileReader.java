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

package org.apache.flink.table.storage.file.lsm.sst;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.file.lsm.KeySortedIterator;
import org.apache.flink.table.storage.file.lsm.LsmIterator;
import org.apache.flink.table.storage.file.lsm.ValueKind;
import org.apache.flink.table.storage.file.utils.OffsetRowData;

import java.io.IOException;

/** TODO {@link KeySortedIterator} will copy records, we need avoid this. */
public class SstFileReader {

    private final Configuration configuration;
    private final BulkFormat<RowData, FileSourceSplit> readerFactory;
    private final int keyArity;
    private final int valueArity;
    private final TypeSerializer<RowData> keySerializer;
    private final TypeSerializer<RowData> valueSerializer;

    public SstFileReader(
            BulkFormat<RowData, FileSourceSplit> readerFactory,
            int keyArity,
            int valueArity,
            TypeSerializer<RowData> keySerializer,
            TypeSerializer<RowData> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.configuration = new Configuration();
        this.readerFactory = readerFactory;
        this.keyArity = keyArity;
        this.valueArity = valueArity;
    }

    public LsmIterator read(Path path) throws IOException {
        FileSourceSplit split =
                new FileSourceSplit(
                        "DUMMY", path, 0, path.getFileSystem().getFileStatus(path).getLen());
        BulkFormat.Reader<RowData> reader = readerFactory.createReader(configuration, split);

        return new LsmIterator() {

            private final OffsetRowData key = new OffsetRowData(keyArity, 2);
            private final OffsetRowData value = new OffsetRowData(valueArity, 2 + keyArity);

            private BulkFormat.RecordIterator<RowData> batchIterator = reader.readBatch();
            private RowData row;

            @Override
            public boolean advanceNext() throws IOException {
                if (batchIterator == null) {
                    return false;
                }

                RecordAndPosition<RowData> record = batchIterator.next();
                if (record != null) {
                    row = record.getRecord();
                    return true;
                } else {
                    batchIterator.releaseBatch();
                    batchIterator = reader.readBatch();
                    return advanceNext();
                }
            }

            @Override
            public long sequenceNumber() {
                return row.getLong(0);
            }

            @Override
            public ValueKind valueKind() {
                return ValueKind.fromByteValue(row.getByte(1));
            }

            @Override
            public RowData key() {
                // TODO remove copy
                return keySerializer.copy(key.replace(row));
            }

            @Override
            public RowData value() {
                // TODO remove copy
                return valueSerializer.copy(value.replace(row));
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }
}
