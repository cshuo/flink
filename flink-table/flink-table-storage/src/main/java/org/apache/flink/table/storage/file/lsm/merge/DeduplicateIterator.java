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

package org.apache.flink.table.storage.file.lsm.merge;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.file.lsm.LsmIterator;

import java.io.IOException;
import java.util.Comparator;

/** */
public class DeduplicateIterator extends AbstractMergeIterator {

    private final Comparator<RowData> comparator;

    private boolean empty = false;

    public DeduplicateIterator(LsmIterator iter, Comparator<RowData> comparator) {
        super(iter);
        this.comparator = comparator;
    }

    @Override
    public boolean advanceNext() throws IOException {
        if (empty) {
            return false;
        }

        // Prepare first record candidate
        if (key == null) {
            if (!iter.advanceNext()) {
                return false;
            }
        }

        // Determine current element
        assignRecord();

        while (true) {
            if (iter.advanceNext()) {
                // ignore same key
                if (comparator.compare(iter.key(), key) == 0) {
                    assignRecord();
                } else {
                    return true;
                }
            } else {
                empty = true;
                // return last record
                return true;
            }
        }
    }
}
