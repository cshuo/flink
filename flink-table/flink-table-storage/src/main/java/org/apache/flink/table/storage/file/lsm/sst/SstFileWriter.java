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

package org.apache.flink.table.storage.file.lsm.sst;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.storage.file.lsm.KeyValue;
import org.apache.flink.table.storage.file.lsm.StoreKey;
import org.apache.flink.table.storage.file.utils.AdvanceIterator;
import org.apache.flink.table.storage.file.utils.FileFactory;
import org.apache.flink.table.storage.file.utils.FileUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** */
public class SstFileWriter {

    private final BulkWriter.Factory<RowData> writerFactory;
    private final FileFactory fileFactory;
    private final long maxFileSize;
    private final TypeSerializer<RowData> keySerializer;

    public SstFileWriter(
            BulkWriter.Factory<RowData> writerFactory,
            FileFactory fileFactory,
            long maxFileSize,
            TypeSerializer<RowData> keySerializer) {
        this.writerFactory = writerFactory;
        this.fileFactory = fileFactory;
        this.maxFileSize = maxFileSize;
        this.keySerializer = keySerializer;
    }

    public List<SstFileMeta> write(
            AdvanceIterator<KeyValue> orderedIter, int level, boolean allowRolling)
            throws IOException {
        List<SstFileMeta> files = new ArrayList<>();

        Path file = null;
        FSDataOutputStream out = null;
        BulkWriter<RowData> writer = null;

        long rowCount = 0;
        StoreKey minKey = null;
        StoreKey maxKey = null;
        long minSequenceNumber = Long.MAX_VALUE;
        long maxSequenceNumber = Long.MIN_VALUE;

        JoinedRowData keyWithMeta = new JoinedRowData();
        JoinedRowData row = new JoinedRowData();
        GenericRowData meta = new GenericRowData(2);

        while (orderedIter.advanceNext()) {
            if (writer == null || (allowRolling && shouldRollOnEvent(out))) {
                if (writer != null) {
                    writer.finish();
                    files.add(
                            new SstFileMeta(
                                    file.getName(),
                                    FileUtils.fileSize(file),
                                    rowCount,
                                    minKey,
                                    maxKey,
                                    minSequenceNumber,
                                    maxSequenceNumber,
                                    level));
                }

                file = fileFactory.newFile();
                out = file.getFileSystem().create(file, FileSystem.WriteMode.NO_OVERWRITE);
                writer = writerFactory.create(out);

                rowCount = 0;
                minKey =
                        new StoreKey(
                                keySerializer.copy(orderedIter.current().key()),
                                orderedIter.current().sequenceNumber(),
                                orderedIter.current().valueKind());
                minSequenceNumber = Long.MAX_VALUE;
                maxSequenceNumber = Long.MIN_VALUE;
            }

            long sequenceNumber = orderedIter.current().sequenceNumber();
            meta.setField(0, sequenceNumber);
            meta.setField(1, orderedIter.current().valueKind().toByteValue());
            row.replace(
                    keyWithMeta.replace(orderedIter.current().key(), meta),
                    orderedIter.current().value());
            writer.addElement(row);

            rowCount++;

            if (sequenceNumber < minSequenceNumber) {
                minSequenceNumber = sequenceNumber;
            }

            if (sequenceNumber > maxSequenceNumber) {
                maxSequenceNumber = sequenceNumber;
            }

            maxKey =
                    new StoreKey(
                            keySerializer.copy(orderedIter.current().key()),
                            sequenceNumber,
                            orderedIter.current().valueKind());
        }

        if (writer != null) {
            writer.finish();
            files.add(
                    new SstFileMeta(
                            file.getName(),
                            FileUtils.fileSize(file),
                            rowCount,
                            minKey,
                            maxKey,
                            minSequenceNumber,
                            maxSequenceNumber,
                            level));
        }

        return files;
    }

    private boolean shouldRollOnEvent(FSDataOutputStream out) throws IOException {
        // This not includes memory size in the writer buffer.
        // The final size of File will be larger than maxFileSize.
        // TODO More accurate to get data size from writer: see ParquetWriter.getDataSize.
        return out.getPos() > maxFileSize;
    }

    public static RowType schema(RowType keyType, RowType valueType) {
        List<RowType.RowField> fields =
                new ArrayList<>(
                        keyType.getFields().stream()
                                .map(f -> new RowType.RowField("_PK_" + f.getName(), f.getType()))
                                .collect(Collectors.toList()));
        fields.add(new RowType.RowField("_SEQUENCE_NUMBER", new BigIntType(false)));
        fields.add(new RowType.RowField("_VALUE_KIND", new TinyIntType(false)));
        fields.addAll(valueType.getFields());
        return new RowType(fields);
    }
}
