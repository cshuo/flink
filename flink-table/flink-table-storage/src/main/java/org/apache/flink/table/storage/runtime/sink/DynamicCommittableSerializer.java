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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** */
public class DynamicCommittableSerializer implements SimpleVersionedSerializer<DynamicCommittable> {

    private final TypeSerializer<RowData> keySerializer;

    public DynamicCommittableSerializer(TypeSerializer<RowData> keySerializer) {
        this.keySerializer = keySerializer;
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(DynamicCommittable obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper view = new DataOutputViewStreamWrapper(out);

        List<ManifestEntry> entries = obj.getEntries();
        view.writeInt(entries.size());
        for (ManifestEntry entry : entries) {
            entry.serialize(view, keySerializer);
        }
        return out.toByteArray();
    }

    @Override
    public DynamicCommittable deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer input = new DataInputDeserializer(serialized);
        int size = input.readInt();
        List<ManifestEntry> entries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            entries.add(ManifestEntry.deserialize(input, keySerializer));
        }
        return new DynamicCommittable(entries);
    }
}
