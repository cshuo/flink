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

package org.apache.flink.table.storage.filestore.lsm.merge;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.filestore.lsm.KeyValue;
import org.apache.flink.table.storage.filestore.utils.AdvanceIterator;
import org.apache.flink.table.storage.filestore.utils.DualIterator;

import java.io.IOException;
import java.util.Comparator;

import static org.apache.flink.util.Preconditions.checkArgument;

/** */
public class CountIterator implements AdvanceIterator<KeyValue> {

    private final DualIterator<KeyValue> iterator;
    private final Comparator<RowData> comparator;

    private boolean firstAdvanced = false;
    private boolean alreadyEmpty = false;

    private long count;

    public CountIterator(DualIterator<KeyValue> iterator, Comparator<RowData> comparator) {
        this.iterator = iterator;
        this.comparator = comparator;
    }

    @Override
    public boolean advanceNext() throws IOException {
        if (alreadyEmpty) {
            return false;
        }

        // Prepare first record candidate
        if (!firstAdvanced) {
            firstAdvanced = true;
            if (!iterator.advanceNext()) {
                return false;
            }
        }

        // assign
        this.count = currentCount();

        while (true) {
            if (iterator.advanceNext()) {
                // accumulate same key
                if (comparator.compare(iterator.current().key(), iterator.previous().key()) == 0) {
                    this.count += currentCount();
                } else {
                    return true;
                }
            } else {
                alreadyEmpty = true;
                // return last record if count is not zero
                return currentCount() != 0;
            }
        }
    }

    @Override
    public KeyValue current() {
        return iterator.previous().setValue(GenericRowData.of(count));
    }

    private long currentCount() {
        RowData value = iterator.current().value();
        checkArgument(!value.isNullAt(0), "The count can not be null.");
        return value.getLong(0);
    }

    @Override
    public void close() throws IOException {
        this.iterator.close();
    }
}
