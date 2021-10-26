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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.filestore.utils.AdvanceIterator;

import java.io.IOException;

/** */
public class UserKeyValueIterator implements KeyValueIterator<RowData, RowData> {

    private final AdvanceIterator<KeyValue> iter;

    public UserKeyValueIterator(AdvanceIterator<KeyValue> iter) {
        this.iter = iter;
    }

    @Override
    public boolean advanceNext() throws IOException {
        while (true) {
            if (!iter.advanceNext()) {
                return false;
            }

            if (iter.current().valueKind() != ValueKind.DELETE) {
                return true;
            }
        }
    }

    @Override
    public RowData getKey() {
        return iter.current().key();
    }

    @Override
    public RowData getValue() {
        return iter.current().value();
    }

    @Override
    public void close() throws IOException {
        iter.close();
    }
}
