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
import org.apache.flink.table.storage.file.lsm.ValueKind;

import java.io.IOException;

/** */
public abstract class AbstractMergeIterator implements LsmIterator {

    protected final LsmIterator iter;

    protected RowData key;
    protected RowData value;
    protected long sequenceNumber;
    protected ValueKind valueKind;

    public AbstractMergeIterator(LsmIterator iter) {
        this.iter = iter;
    }

    protected void assignRecord() {
        key = iter.key();
        value = iter.value();
        sequenceNumber = iter.sequenceNumber();
        valueKind = iter.valueKind();
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
        iter.close();
    }
}
