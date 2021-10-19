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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** */
public class StoreKey {

    public static final int FIELD_COUNT = 3;

    private final RowData userKey;
    private final long sequenceNumber;
    private final ValueKind valueKind;

    public StoreKey(RowData userKey, long sequenceNumber, ValueKind valueKind) {
        this.userKey = userKey;
        this.sequenceNumber = sequenceNumber;
        this.valueKind = valueKind;
    }

    public RowData getUserKey() {
        return userKey;
    }

    public long getsequenceNumber() {
        return sequenceNumber;
    }

    public ValueKind getValueKind() {
        return valueKind;
    }

    public void serialize(DataOutputView target, TypeSerializer<RowData> keySerializer)
            throws IOException {
        keySerializer.serialize(userKey, target);
        target.writeLong(sequenceNumber);
        target.writeByte(valueKind.toByteValue());
    }

    public static StoreKey deserialize(DataInputView in, TypeSerializer<RowData> keySerializer)
            throws IOException {
        return new StoreKey(
                keySerializer.deserialize(in),
                in.readLong(),
                ValueKind.fromByteValue(in.readByte()));
    }

    public RowData toRow() {
        GenericRowData row = new GenericRowData(3);
        row.setField(0, userKey);
        row.setField(1, sequenceNumber);
        row.setField(2, valueKind.toByteValue());
        return row;
    }

    public static RowType schema(RowType keyType) {
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("_USER_KEY", keyType));
        fields.add(new RowType.RowField("_SEQUENCE_NUMBER", new BigIntType(false)));
        fields.add(new RowType.RowField("_VALUE_KIND", new TinyIntType(false)));
        return new RowType(fields);
    }

    public static StoreKey fromRow(RowData row, int keyArity, TypeSerializer<RowData> keySer) {
        return new StoreKey(
                keySer.copy(row.getRow(0, keyArity)),
                row.getLong(1),
                ValueKind.fromByteValue(row.getByte(2)));
    }
}
