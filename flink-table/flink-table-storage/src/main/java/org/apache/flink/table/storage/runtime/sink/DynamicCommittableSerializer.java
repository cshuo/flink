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

package org.apache.flink.table.storage.runtime.sink;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.file.manifest.ManifestEntry;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** */
public class DynamicCommittableSerializer<LogCommT>
        implements SimpleVersionedSerializer<DynamicCommittable<LogCommT>> {

    private final TypeSerializer<RowData> keySerializer;
    @Nullable private final SimpleVersionedSerializer<LogCommT> logSerializer;

    public DynamicCommittableSerializer(
            TypeSerializer<RowData> keySerializer,
            @Nullable SimpleVersionedSerializer<LogCommT> logSerializer) {
        this.keySerializer = keySerializer;
        this.logSerializer = logSerializer;
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(DynamicCommittable<LogCommT> obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);

        view.writeInt(obj.getLogCommittables().size());
        for (LogCommT comm : obj.getLogCommittables()) {
            byte[] logCommBytes = logSerializer.serialize(comm);
            view.writeInt(logCommBytes.length);
            view.write(logCommBytes);
        }

        view.writeInt(obj.getLogOffsets().size());
        for (Map.Entry<Integer, Long> entry : obj.getLogOffsets().entrySet()) {
            view.writeInt(entry.getKey());
            view.writeLong(entry.getValue());
        }

        List<ManifestEntry> entries = obj.getEntries();
        view.writeInt(entries.size());
        for (ManifestEntry entry : entries) {
            entry.serialize(view, keySerializer);
        }
        return out.toByteArray();
    }

    @Override
    public DynamicCommittable<LogCommT> deserialize(int version, byte[] serialized)
            throws IOException {
        DataInputDeserializer input = new DataInputDeserializer(serialized);
        int size = input.readInt();
        List<LogCommT> logCommittables = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            int len = input.readInt();
            byte[] bytes = new byte[len];
            input.read(bytes);
            // TODO version of log ser?
            logCommittables.add(logSerializer.deserialize(logSerializer.getVersion(), bytes));
        }

        int offsetSize = input.readInt();
        Map<Integer, Long> logOffsets = new HashMap<>();
        for (int i = 0; i < offsetSize; i++) {
            logOffsets.put(input.readInt(), input.readLong());
        }

        int entriesSize = input.readInt();
        List<ManifestEntry> entries = new ArrayList<>(entriesSize);
        for (int i = 0; i < entriesSize; i++) {
            entries.add(ManifestEntry.deserialize(input, keySerializer));
        }
        return new DynamicCommittable<>(logCommittables, logOffsets, entries);
    }
}
