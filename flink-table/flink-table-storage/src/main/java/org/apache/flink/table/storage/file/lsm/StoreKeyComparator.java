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

/** */
public class StoreKeyComparator implements Comparator<StoreKey> {

    private final Comparator<RowData> userComparator;

    public StoreKeyComparator(Comparator<RowData> comparator) {
        this.userComparator = comparator;
    }

    @Override
    public int compare(StoreKey left, StoreKey right) {
        return compare(
                left.getUserKey(),
                left.getsequenceNumber(),
                right.getUserKey(),
                right.getsequenceNumber());
    }

    public int compare(RowData k1, long sequenceNumber1, RowData k2, long sequenceNumber2) {
        int result = userComparator.compare(k1, k2);
        if (result != 0) {
            return result;
        }

        return Long.compare(sequenceNumber1, sequenceNumber2);
    }
}
