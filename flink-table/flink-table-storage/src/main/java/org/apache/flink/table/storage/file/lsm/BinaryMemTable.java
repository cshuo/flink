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

package org.apache.flink.table.storage.file.lsm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.operators.sort.IndexedSorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.plan.utils.SortUtil;
import org.apache.flink.table.runtime.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.aggregate.BytesHashMapSpillMemorySegmentPool;
import org.apache.flink.table.runtime.operators.sort.BinaryKVInMemorySortBuffer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.collections.binary.BytesHashMap;
import org.apache.flink.table.runtime.util.collections.binary.BytesMap;
import org.apache.flink.table.storage.file.utils.DualIterator;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BinaryMemTable implements MemTable {

    private final BytesHashMap table;

    // keyType + long + byte
    private final RowType storeKeyType;
    private final RowType valueType;
    private final int userKeyLength;
    private final RowDataSerializer storeKeySerializer;
    private final RowDataSerializer valueSerializer;

    private final GenericRowData reuseMetaRow;
    private final JoinedRowData reuseStoreRow;

    // for sort binary buffer
    private final NormalizedKeyComputer normalizedKeyComputer;
    private final RecordComparator keyComparator;
    private final Projection<RowData, BinaryRowData> projection;

    public BinaryMemTable(RowType userKeyType, RowType valueType, long maxMemSize) {
        List<LogicalType> types =
                userKeyType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .collect(Collectors.toList());
        types.add(new BigIntType());
        types.add(new TinyIntType());
        LogicalType[] keyTypes = types.toArray(new LogicalType[0]);
        LogicalType[] valTypes =
                valueType.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        this.table = new BytesHashMap(null, null, maxMemSize, keyTypes, valTypes);

        this.userKeyLength = userKeyType.getFieldCount();
        this.storeKeyType = RowType.of(types.toArray(new LogicalType[0]));
        this.valueType = valueType;
        this.storeKeySerializer = new RowDataSerializer(storeKeyType);
        this.valueSerializer = new RowDataSerializer(valueType);
        this.reuseMetaRow = new GenericRowData(2);
        this.reuseStoreRow = new JoinedRowData();

        // for sort binary buffer
        SortCodeGenerator sortCodeGenerator =
                new SortCodeGenerator(
                        new TableConfig(),
                        storeKeyType,
                        SortUtil.getAscendingSortSpec(
                                IntStream.range(0, storeKeyType.getFieldCount()).toArray()));
        GeneratedNormalizedKeyComputer genKeyComputer =
                sortCodeGenerator.generateNormalizedKeyComputer("MapKeyComputer");
        GeneratedRecordComparator genRecordComparator =
                sortCodeGenerator.generateRecordComparator("MapRecordComparator");
        GeneratedProjection genProjection =
                ProjectionCodeGenerator.generateProjection(
                        CodeGeneratorContext.apply(new TableConfig()),
                        "UserKeyProjection",
                        storeKeyType,
                        userKeyType,
                        IntStream.range(0, userKeyType.getFieldCount()).toArray());
        this.normalizedKeyComputer =
                genKeyComputer.newInstance(Thread.currentThread().getContextClassLoader());
        this.keyComparator =
                genRecordComparator.newInstance(Thread.currentThread().getContextClassLoader());
        //noinspection unchecked
        this.projection = genProjection.newInstance(Thread.currentThread().getContextClassLoader());
    }

    @Override
    public void put(long sequenceNumber, ValueKind valueType, RowData key, RowData value) {
        reuseMetaRow.setField(0, sequenceNumber);
        reuseMetaRow.setField(1, valueType.toByteValue());
        reuseStoreRow.replace(key, reuseMetaRow);

        BytesMap.LookupInfo<BinaryRowData, BinaryRowData> lookupInfo =
                table.lookup(storeKeySerializer.toBinaryRow(reuseStoreRow));
        assert !lookupInfo.isFound();
        try {
            table.append(lookupInfo, valueSerializer.toBinaryRow(value));
        } catch (IOException e) {
            throw new StoreException("MemTable is full, cannot add any more records.");
        }
    }

    @Override
    public int size() {
        return (int) table.getNumElements();
    }

    @Override
    public DualIterator<KeyValue> iterator() {
        try {
            BinaryKVInMemorySortBuffer sortBuffer =
                    BinaryKVInMemorySortBuffer.createBuffer(
                            normalizedKeyComputer,
                            new BinaryRowDataSerializer(storeKeyType.getFieldCount()),
                            new BinaryRowDataSerializer(valueType.getFieldCount()),
                            keyComparator,
                            table.getRecordAreaMemorySegments(),
                            table.getNumElements(),
                            new BytesHashMapSpillMemorySegmentPool(
                                    table.getBucketAreaMemorySegments()));

            final IndexedSorter sorter = new QuickSort();

            // sort the binary buffer
            sorter.sort(sortBuffer);

            MutableObjectIterator<Tuple2<BinaryRowData, BinaryRowData>> kvIter =
                    sortBuffer.getIterator();

            return new DualIterator<KeyValue>() {

                private KeyValue previous = new KeyValue();
                private KeyValue current = new KeyValue();

                private BinaryRowData reuseKey =
                        (BinaryRowData) storeKeySerializer.createInstance();
                private BinaryRowData reuseValue = (BinaryRowData) valueSerializer.createInstance();

                @Override
                public boolean advanceNext() throws IOException {
                    switchKeyValue();

                    Tuple2<BinaryRowData, BinaryRowData> kv =
                            kvIter.next(Tuple2.of(reuseKey, reuseValue));
                    if (kv == null) {
                        return false;
                    }
                    BinaryRowData userKey = projection.apply(reuseKey).copy();
                    long sequenceNumber = reuseKey.getLong(userKeyLength);
                    ValueKind valueKind =
                            ValueKind.fromByteValue(reuseKey.getByte(userKeyLength + 1));
                    current.replace(
                            new StoreKey(userKey, sequenceNumber, valueKind), reuseValue.copy());
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
        } catch (IOException e) {
            throw new StoreException("Exception happened when flush MemTable.", e);
        }
    }

    @Override
    public void clear() {
        table.reset();
    }
}
