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

package org.apache.flink.table.storage.file.manifest;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.storage.file.lsm.StoreKey;
import org.apache.flink.table.storage.file.lsm.sst.SstFileMeta;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.storage.file.utils.TableUtils.nullableDeserialize;
import static org.apache.flink.table.storage.file.utils.TableUtils.nullableSerialize;

/** */
public class ManifestEntry {

    private final FileKind kind;

    private final String partition;

    private final int bucket;

    private final SstFileMeta file;

    public ManifestEntry(FileKind kind, String partition, int bucket, SstFileMeta file) {
        this.kind = kind;
        this.partition = partition;
        this.bucket = bucket;
        this.file = file;
    }

    public FileKind kind() {
        return kind;
    }

    public String partition() {
        return partition;
    }

    public int bucket() {
        return bucket;
    }

    public String name() {
        return file.getName();
    }

    public SstFileMeta file() {
        return file;
    }

    public RowData toRow() {
        GenericRowData row = new GenericRowData(11);
        row.setField(0, kind.toByteValue());
        row.setField(1, StringData.fromString(partition));
        row.setField(2, bucket);
        row.setField(3, StringData.fromString(file.getName()));
        row.setField(4, file.getFileSize());
        row.setField(5, file.getRowCount());
        row.setField(6, file.getMinKey().toRow());
        row.setField(7, file.getMaxKey().toRow());
        row.setField(8, file.getMinSequenceNumber());
        row.setField(9, file.getMaxSequenceNumber());
        row.setField(10, file.getLevel());
        return row;
    }

    public static RowType schema(RowType keyType) {
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("_FILE_KIND", new TinyIntType(false)));
        fields.add(new RowType.RowField("_PARTITION", new VarCharType(Integer.MAX_VALUE)));
        fields.add(new RowType.RowField("_BUCKET", new IntType(false)));
        fields.add(new RowType.RowField("_FILE_NAME", new VarCharType(false, Integer.MAX_VALUE)));
        fields.add(new RowType.RowField("_FILE_SIZE", new BigIntType(false)));
        fields.add(new RowType.RowField("_ROW_COUNT", new BigIntType(false)));
        fields.add(new RowType.RowField("_MIN_KEY", StoreKey.schema(keyType)));
        fields.add(new RowType.RowField("_MAX_KEY", StoreKey.schema(keyType)));
        fields.add(new RowType.RowField("_MIN_SEQUENCE_NUMBER", new BigIntType(false)));
        fields.add(new RowType.RowField("_MAX_SEQUENCE_NUMBER", new BigIntType(false)));
        fields.add(new RowType.RowField("_LEVEL", new IntType(false)));
        return new RowType(fields);
    }

    public static ManifestEntry fromRow(RowData row, int keyArity, TypeSerializer<RowData> keySer) {
        return new ManifestEntry(
                FileKind.fromByteValue(row.getByte(0)),
                row.isNullAt(1) ? null : row.getString(1).toString(),
                row.getInt(2),
                new SstFileMeta(
                        row.getString(3).toString(),
                        row.getLong(4),
                        row.getLong(5),
                        StoreKey.fromRow(row.getRow(6, StoreKey.FIELD_COUNT), keyArity, keySer),
                        StoreKey.fromRow(row.getRow(7, StoreKey.FIELD_COUNT), keyArity, keySer),
                        row.getLong(8),
                        row.getLong(9),
                        row.getInt(10)));
    }

    public void serialize(DataOutputView target, TypeSerializer<RowData> keySerializer)
            throws IOException {
        target.writeByte(kind.toByteValue());
        nullableSerialize(target, partition);
        target.writeInt(bucket);
        file.serialize(target, keySerializer);
    }

    public static ManifestEntry deserialize(DataInputView in, TypeSerializer<RowData> keySerializer)
            throws IOException {
        return new ManifestEntry(
                FileKind.fromByteValue(in.readByte()),
                nullableDeserialize(in),
                in.readInt(),
                SstFileMeta.deserialize(in, keySerializer));
    }

    public FileIdentifier identifier() {
        return new FileIdentifier(partition, bucket, name());
    }
}
