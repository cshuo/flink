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

package org.apache.flink.table.storage.runtime.source;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.filestore.lsm.sst.SstFileMeta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.storage.filestore.utils.TableUtils.nullableDeserialize;
import static org.apache.flink.table.storage.filestore.utils.TableUtils.nullableSerialize;

/** */
public class DynamicSplitSerializer implements SimpleVersionedSerializer<DynamicSplit> {

    private final TypeSerializer<RowData> keySerializer;

    public DynamicSplitSerializer(TypeSerializer<RowData> keySerializer) {
        this.keySerializer = keySerializer;
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(DynamicSplit split) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(20);
        nullableSerialize(out, split.getPartition());
        out.writeInt(split.getBucketId());
        out.writeInt(split.getFiles().size());
        for (SstFileMeta file : split.getFiles()) {
            file.serialize(out, keySerializer);
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public DynamicSplit deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        String partition = nullableDeserialize(in);
        int bucket = in.readInt();
        int fileSize = in.readInt();
        List<SstFileMeta> files = new ArrayList<>(fileSize);
        for (int i = 0; i < fileSize; i++) {
            files.add(SstFileMeta.deserialize(in, keySerializer));
        }
        return new DynamicSplit(partition, bucket, files);
    }
}
