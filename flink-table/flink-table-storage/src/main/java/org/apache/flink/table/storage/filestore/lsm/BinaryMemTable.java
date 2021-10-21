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

package org.apache.flink.table.storage.filestore.lsm;

import org.apache.flink.runtime.operators.sort.IndexedSorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.plan.utils.SortUtil;
import org.apache.flink.table.runtime.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.LazyMemorySegmentPool;
import org.apache.flink.table.storage.filestore.lsm.sst.SstFileWriter;
import org.apache.flink.table.storage.filestore.utils.DualIterator;
import org.apache.flink.table.storage.filestore.utils.OffsetRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.util.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** todo */
public class BinaryMemTable implements MemTable {

    private final BinaryInMemorySortBuffer buffer;

    private final int userKeyArity;
    private final int valueArity;

    private final GenericRowData reuseMetaRow;
    private final JoinedRowData reuseStoreRow;
    private final JoinedRowData reuseRow;

    public BinaryMemTable(RowType userKeyType, RowType valueType, long maxMemSize) {
        this.userKeyArity = userKeyType.getFieldCount();
        this.valueArity = valueType.getFieldCount();
        this.reuseMetaRow = new GenericRowData(2);
        this.reuseStoreRow = new JoinedRowData();
        this.reuseRow = new JoinedRowData();

        RowType storeKeyType = storeKeySchema(userKeyType);
        RowType rowType = SstFileWriter.schema(userKeyType, valueType);
        RowDataSerializer rowSerializer = new RowDataSerializer(rowType);

        // for sort binary buffer
        SortCodeGenerator sortCodeGenerator =
                new SortCodeGenerator(
                        new TableConfig(),
                        storeKeyType,
                        SortUtil.getAscendingSortSpec(
                                IntStream.range(0, storeKeyType.getFieldCount()).toArray()));
        GeneratedNormalizedKeyComputer genKeyComputer =
                sortCodeGenerator.generateNormalizedKeyComputer("RecordKeyComputer");
        GeneratedRecordComparator genRecordComparator =
                sortCodeGenerator.generateRecordComparator("RecordComparator");
        NormalizedKeyComputer normalizedKeyComputer =
                genKeyComputer.newInstance(Thread.currentThread().getContextClassLoader());
        RecordComparator keyComparator =
                genRecordComparator.newInstance(Thread.currentThread().getContextClassLoader());
        LazyMemorySegmentPool memorySegmentPool = new LazyMemorySegmentPool(null, null, maxMemSize);

        this.buffer =
                BinaryInMemorySortBuffer.createBuffer(
                        normalizedKeyComputer,
                        rowSerializer,
                        new BinaryRowDataSerializer(storeKeyType.getFieldCount()),
                        keyComparator,
                        memorySegmentPool);
    }

    @Override
    public void put(long sequenceNumber, ValueKind valueType, RowData key, RowData value)
            throws IOException {
        reuseMetaRow.setField(0, sequenceNumber);
        reuseMetaRow.setField(1, valueType.toByteValue());
        reuseStoreRow.replace(key, reuseMetaRow);
        reuseRow.replace(reuseStoreRow, value);

        if (!buffer.write(reuseRow)) {
            throw new EOFException("The write-buffer is too small.");
        }
    }

    @Override
    public int size() {
        return buffer.size();
    }

    @Override
    public DualIterator<KeyValue> iterator() {
        final IndexedSorter sorter = new QuickSort();
        // sort the binary buffer
        sorter.sort(buffer);

        MutableObjectIterator<BinaryRowData> kvIter = buffer.getIterator();

        return new DualIterator<KeyValue>() {

            private KeyValue previous = new KeyValue();
            private KeyValue current = new KeyValue();

            @Override
            public boolean advanceNext() throws IOException {
                switchKeyValue();

                BinaryRowData row = kvIter.next();
                if (row == null) {
                    return false;
                }
                current.replace(
                        new OffsetRowData(userKeyArity, 0).replace(row),
                        row.getLong(userKeyArity),
                        ValueKind.fromByteValue(row.getByte(userKeyArity + 1)),
                        new OffsetRowData(valueArity, userKeyArity + 2).replace(row));
                return true;
            }

            @Override
            public KeyValue previous() {
                return previous;
            }

            @Override
            public KeyValue current() {
                return current;
            }

            private void switchKeyValue() {
                KeyValue tmp = previous;
                previous = current;
                current = tmp;
            }

            @Override
            public void close() throws IOException {}
        };
    }

    @Override
    public void clear() {
        buffer.reset();
    }

    private RowType storeKeySchema(RowType keyType) {
        List<RowType.RowField> fields =
                new ArrayList<>(
                        keyType.getFields().stream()
                                .map(f -> new RowType.RowField("_PK_" + f.getName(), f.getType()))
                                .collect(Collectors.toList()));
        fields.add(new RowType.RowField("_SEQUENCE_NUMBER", new BigIntType(false)));
        fields.add(new RowType.RowField("_VALUE_KIND", new TinyIntType(false)));
        return new RowType(fields);
    }
}
