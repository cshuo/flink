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
import org.apache.flink.table.storage.file.lsm.KeyValue;
import org.apache.flink.table.storage.file.lsm.ValueKind;
import org.apache.flink.table.storage.file.utils.DualIterator;
import org.apache.flink.table.storage.file.utils.OffsetRowData;

import java.io.IOException;

/** */
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

    public DualIterator<KeyValue> read(Path path) throws IOException {
        FileSourceSplit split =
                new FileSourceSplit(
                        "DUMMY", path, 0, path.getFileSystem().getFileStatus(path).getLen());
        BulkFormat.Reader<RowData> reader = readerFactory.createReader(configuration, split);

        return new DualIterator<KeyValue>() {

            private KeyValue previous = new KeyValue();
            private KeyValue current = new KeyValue();

            private BulkFormat.RecordIterator<RowData> batchIterator = reader.readBatch();

            private void switchKeyValue() {
                KeyValue tmp = previous;
                previous = current;
                current = tmp;
            }

            @Override
            public boolean advanceNext() throws IOException {
                if (batchIterator == null) {
                    switchKeyValue();
                    return false;
                }

                RecordAndPosition<RowData> record = batchIterator.next();
                if (record != null) {
                    switchKeyValue();
                    RowData row = record.getRecord();
                    // TODO avoid copy
                    current.replace(
                            keySerializer.copy(new OffsetRowData(keyArity, 0).replace(row)),
                            row.getLong(keyArity),
                            ValueKind.fromByteValue(row.getByte(keyArity + 1)),
                            valueSerializer.copy(
                                    new OffsetRowData(valueArity, keyArity + 2).replace(row)));
                    return true;
                } else {
                    batchIterator.releaseBatch();
                    batchIterator = reader.readBatch();
                    return advanceNext();
                }
            }

            @Override
            public KeyValue previous() {
                return previous;
            }

            @Override
            public KeyValue current() {
                return current;
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }
}
