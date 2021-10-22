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

package org.apache.flink.table.storage.runtime.pk;

import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.file.lsm.KeyValueIterator;
import org.apache.flink.table.storage.file.lsm.StoreException;
import org.apache.flink.table.storage.runtime.RowReader;
import org.apache.flink.util.CloseableIterator;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/** */
public class PrimaryKeyRowReader implements RowReader {

    @Override
    public CloseableIterator<RecordAndPosition<RowData>> readRecords(
            KeyValueIterator<RowData, RowData> iterator) {
        return new CloseableIterator<RecordAndPosition<RowData>>() {

            private final AtomicLong cnt = new AtomicLong(0);

            private boolean advanced = false;

            @Override
            public boolean hasNext() {
                if (advanced) {
                    return true;
                }

                try {
                    if (iterator.advanceNext()) {
                        advanced = true;
                        return true;
                    }
                } catch (IOException e) {
                    throw new StoreException(e);
                }

                return false;
            }

            @Override
            public RecordAndPosition<RowData> next() {
                if (hasNext()) {
                    advanced = false;
                    return new RecordAndPosition<>(iterator.getValue(), 0, cnt.getAndIncrement());
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public void close() throws Exception {
                iterator.close();
            }
        };
    }
}
