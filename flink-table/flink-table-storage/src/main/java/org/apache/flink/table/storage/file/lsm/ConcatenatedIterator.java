/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

/** */
public class ConcatenatedIterator implements LsmIterator {

    private final LinkedList<Supplier<LsmIterator>> queue;

    private LsmIterator iterator;

    public ConcatenatedIterator(List<Supplier<LsmIterator>> suppliers) throws IOException {
        this.queue = new LinkedList<>(suppliers);
        nextIterator();
    }

    private void nextIterator() throws IOException {
        close();
        Supplier<LsmIterator> supplier = queue.poll();
        this.iterator = supplier == null ? null : supplier.get();
    }

    @Override
    public boolean advanceNext() throws IOException {
        while (true) {
            if (iterator == null) {
                return false;
            }
            boolean next = iterator.advanceNext();
            if (next) {
                return true;
            } else {
                nextIterator();
            }
        }
    }

    @Override
    public long sequenceNumber() {
        return this.iterator.sequenceNumber();
    }

    @Override
    public ValueKind valueKind() {
        return this.iterator.valueKind();
    }

    @Override
    public RowData key() {
        return this.iterator.key();
    }

    @Override
    public RowData value() {
        return this.iterator.value();
    }

    @Override
    public void close() throws IOException {
        if (this.iterator != null) {
            this.iterator.close();
        }
    }
}
