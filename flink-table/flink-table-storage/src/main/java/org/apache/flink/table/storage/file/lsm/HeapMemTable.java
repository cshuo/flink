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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/** */
public class HeapMemTable implements MemTable {

    private final TreeMap<StoreKey, RowData> table;

    public HeapMemTable(Comparator<RowData> comparator) {
        table = new TreeMap<>(new StoreKeyComparator(comparator));
    }

    @Override
    public void put(long sequenceNumber, ValueKind valueType, RowData key, RowData value) {
        table.put(new StoreKey(key, sequenceNumber, valueType), value);
    }

    @Override
    public int size() {
        return table.size();
    }

    @Override
    public LsmIterator iterator() {
        Iterator<Map.Entry<StoreKey, RowData>> entries = table.entrySet().iterator();
        return new LsmIterator() {

            private Map.Entry<StoreKey, RowData> current;

            @Override
            public boolean advanceNext() {
                if (entries.hasNext()) {
                    current = entries.next();
                    return true;
                }
                return false;
            }

            @Override
            public long sequenceNumber() {
                return current.getKey().getsequenceNumber();
            }

            @Override
            public ValueKind valueKind() {
                return current.getKey().getValueKind();
            }

            @Override
            public RowData key() {
                return current.getKey().getUserKey();
            }

            @Override
            public RowData value() {
                return current.getValue();
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public void clear() {
        table.clear();
    }

    @Override
    public StoreKey minKey() {
        return table.firstKey();
    }

    @Override
    public StoreKey maxKey() {
        return table.lastKey();
    }
}
