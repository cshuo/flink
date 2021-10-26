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

package org.apache.flink.table.storage.filestore.lsm.sst;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.ColumnarRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.ColumnarRowIterator;
import org.apache.flink.table.storage.filestore.lsm.KeyValue;
import org.apache.flink.table.storage.filestore.lsm.ValueKind;
import org.apache.flink.table.storage.filestore.utils.DualIterator;
import org.apache.flink.table.storage.filestore.utils.OffsetRowData;

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

        BulkFormat.RecordIterator<RowData> firstIterator = reader.readBatch();

        // In case of column storage, the following optimization will be done to achieve the effect
        // of zero copy.
        boolean isColumnar = firstIterator instanceof ColumnarRowIterator;

        return new DualIterator<KeyValue>() {

            private BulkFormat.RecordIterator<RowData> batchIterator = firstIterator;

            private ColumnarRowData previousColumnarRow = new ColumnarRowData();
            private KeyValue previous = createKeyValue(previousColumnarRow);

            private ColumnarRowData currentColumnarRow = new ColumnarRowData();
            private KeyValue current = createKeyValue(currentColumnarRow);

            private KeyValue copiedPrevious;

            private void switchKeyValue(boolean newBatch) {
                KeyValue tmp = previous;
                previous = current;
                current = tmp;

                ColumnarRowData tmpRow = previousColumnarRow;
                previousColumnarRow = currentColumnarRow;
                currentColumnarRow = tmpRow;

                if (!newBatch) {
                    copiedPrevious = null;
                }
            }

            private KeyValue createKeyValue(ColumnarRowData columnarRow) {
                if (isColumnar) {
                    // create wrappers for ColumnarRowData
                    OffsetRowData key = new OffsetRowData(keyArity, 0).replace(columnarRow);
                    OffsetRowData value =
                            new OffsetRowData(valueArity, keyArity + 2).replace(columnarRow);
                    return new KeyValue().replace(key, -1, null, value);
                }

                return new KeyValue();
            }

            @Override
            public boolean advanceNext() throws IOException {
                return advanceNext(false);
            }

            private boolean advanceNext(boolean newBatch) throws IOException {
                if (batchIterator == null) {
                    switchKeyValue(newBatch);
                    return false;
                }

                RecordAndPosition<RowData> record = batchIterator.next();
                if (record != null) {
                    switchKeyValue(newBatch);
                    setCurrent(record.getRecord());
                    return true;
                } else {
                    copiedPrevious =
                            new KeyValue()
                                    .replace(
                                            keySerializer.copy(current.key()),
                                            current.sequenceNumber(),
                                            current.valueKind(),
                                            valueSerializer.copy((current.value())));
                    batchIterator.releaseBatch();
                    batchIterator = reader.readBatch();
                    return advanceNext(true);
                }
            }

            private void setCurrent(RowData row) {
                long sequenceNumber = row.getLong(keyArity);
                ValueKind valueKind = ValueKind.fromByteValue(row.getByte(keyArity + 1));

                if (isColumnar) {
                    ColumnarRowData copyFrom = (ColumnarRowData) row;
                    currentColumnarRow.setVectorizedColumnBatch(
                            copyFrom.getVectorizedColumnBatch());
                    currentColumnarRow.setRowId(copyFrom.getRowId());
                    current.setSequenceNumber(sequenceNumber);
                    current.setValueKind(valueKind);
                } else {
                    current.replace(
                            keySerializer.copy(new OffsetRowData(keyArity, 0).replace(row)),
                            row.getLong(keyArity),
                            valueKind,
                            valueSerializer.copy(
                                    new OffsetRowData(valueArity, keyArity + 2).replace(row)));
                }
            }

            @Override
            public KeyValue previous() {
                return copiedPrevious != null ? copiedPrevious : previous;
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
