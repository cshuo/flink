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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.file.utils.DualIterator;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/** */
public class HeapMemTable implements MemTable {

    private final TreeMap<StoreKey, RowData> table;
    private final int maxMemRecords;

    public HeapMemTable(Comparator<RowData> comparator, int maxMemRecords) {
        table = new TreeMap<>(new StoreKeyComparator(comparator));
        this.maxMemRecords = maxMemRecords;
    }

    @Override
    public void put(long sequenceNumber, ValueKind valueType, RowData key, RowData value) {
        if (table.size() > maxMemRecords) {
            throw new StoreException("MemTable is full, cannot add any more records.");
        }
        table.put(new StoreKey(key, sequenceNumber, valueType), value);
    }

    @Override
    public int size() {
        return table.size();
    }

    @Override
    public DualIterator<KeyValue> iterator() {
        Iterator<Map.Entry<StoreKey, RowData>> entries = table.entrySet().iterator();
        return new DualIterator<KeyValue>() {

            private KeyValue previous = new KeyValue();
            private KeyValue current = new KeyValue();

            private void switchKeyValue() {
                KeyValue tmp = previous;
                previous = current;
                current = tmp;
            }

            @Override
            public boolean advanceNext() {
                switchKeyValue();

                if (entries.hasNext()) {
                    Map.Entry<StoreKey, RowData> entry = entries.next();
                    current.replace(entry.getKey(), entry.getValue());
                    return true;
                }
                return false;
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
            public void close() {}
        };
    }

    @Override
    public void clear() {
        table.clear();
    }
}
