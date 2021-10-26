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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.filestore.lsm.KeyValue;
import org.apache.flink.table.storage.filestore.utils.AdvanceIterator;
import org.apache.flink.table.storage.filestore.utils.DualIterator;

import java.io.Serializable;
import java.util.Comparator;

/** */
public enum MergePolicy implements Serializable {
    COUNT,
    DEDUPLICATE;

    public AdvanceIterator<KeyValue> merge(
            DualIterator<KeyValue> iterator, Comparator<RowData> comparator) {
        switch (this) {
            case COUNT:
                return new CountIterator(iterator, comparator);
            case DEDUPLICATE:
                return new DeduplicateIterator(iterator, comparator);
            default:
                throw new UnsupportedOperationException("Unsupported strategy: " + this);
        }
    }
}
