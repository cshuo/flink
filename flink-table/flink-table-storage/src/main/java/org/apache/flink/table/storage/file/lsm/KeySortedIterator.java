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

import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;

/** */
public final class KeySortedIterator implements LsmIterator {

    private final PriorityQueue<LsmIterator> priorityQueue;
    private final List<LsmIterator> iterators;

    private RowData key;
    private long sequenceNumber;
    private ValueKind valueKind;
    private RowData value;

    public KeySortedIterator(List<LsmIterator> iterators, StoreKeyComparator comparator)
            throws IOException {
        this.iterators = iterators;

        this.priorityQueue =
                new PriorityQueue<>(
                        iterators.size() + 1,
                        (left, right) ->
                                comparator.compare(
                                        left.key(),
                                        left.sequenceNumber(),
                                        right.key(),
                                        right.sequenceNumber()));
        for (LsmIterator iter : iterators) {
            if (iter.advanceNext()) {
                priorityQueue.add(iter);
            }
        }
    }

    @Override
    public boolean advanceNext() throws IOException {
        LsmIterator iterator = priorityQueue.poll();
        if (iterator != null) {
            key = iterator.key();
            sequenceNumber = iterator.sequenceNumber();
            valueKind = iterator.valueKind();
            value = iterator.value();
            if (iterator.advanceNext()) {
                priorityQueue.add(iterator);
            }
            return true;
        }
        return false;
    }

    @Override
    public long sequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public ValueKind valueKind() {
        return valueKind;
    }

    @Override
    public RowData key() {
        return key;
    }

    @Override
    public RowData value() {
        return value;
    }

    @Override
    public void close() throws IOException {
        for (LsmIterator iter : iterators) {
            iter.close();
        }
    }
}
