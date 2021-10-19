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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.file.lsm.StoreKey;
import org.apache.flink.table.storage.file.lsm.StoreKeyComparator;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/** */
public class SstFileMeta {

    private final String name;

    private final long fileSize;

    private final long rowCount;

    private final StoreKey minKey;

    private final StoreKey maxKey;

    private final long minSequenceNumber;

    private final long maxSequenceNumber;

    private final int level;

    public SstFileMeta(
            String name,
            long fileSize,
            long rowCount,
            StoreKey minKey,
            StoreKey maxKey,
            long minSequenceNumber,
            long maxSequenceNumber,
            int level) {
        this.name = name;
        this.fileSize = fileSize;
        this.rowCount = rowCount;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.minSequenceNumber = minSequenceNumber;
        this.maxSequenceNumber = maxSequenceNumber;
        this.level = level;
    }

    public String getName() {
        return name;
    }

    public long getFileSize() {
        return fileSize;
    }

    public long getRowCount() {
        return rowCount;
    }

    public StoreKey getMinKey() {
        return minKey;
    }

    public StoreKey getMaxKey() {
        return maxKey;
    }

    public long getMinSequenceNumber() {
        return minSequenceNumber;
    }

    public long getMaxSequenceNumber() {
        return maxSequenceNumber;
    }

    public int getLevel() {
        return level;
    }

    public void serialize(DataOutputView target, TypeSerializer<RowData> keySerializer)
            throws IOException {
        target.writeUTF(name);
        target.writeLong(fileSize);
        target.writeLong(rowCount);
        minKey.serialize(target, keySerializer);
        maxKey.serialize(target, keySerializer);
        target.writeLong(minSequenceNumber);
        target.writeLong(maxSequenceNumber);
        target.writeInt(level);
    }

    public SstFileMeta upgrade(int newLevel) {
        checkArgument(newLevel > this.level);
        return new SstFileMeta(
                name,
                fileSize,
                rowCount,
                minKey,
                maxKey,
                minSequenceNumber,
                maxSequenceNumber,
                newLevel);
    }

    public static SstFileMeta deserialize(DataInputView in, TypeSerializer<RowData> keySerializer)
            throws IOException {
        return new SstFileMeta(
                in.readUTF(),
                in.readLong(),
                in.readLong(),
                StoreKey.deserialize(in, keySerializer),
                StoreKey.deserialize(in, keySerializer),
                in.readLong(),
                in.readLong(),
                in.readInt());
    }

    public static StoreKey min(List<SstFileMeta> files, Comparator<RowData> userComparator) {
        StoreKeyComparator comparator = new StoreKeyComparator(userComparator);
        StoreKey min = null;
        for (SstFileMeta meta : files) {
            // TODO remove null check, comparator should deal with it if there is null key.
            if (meta.minKey == null) {
                continue;
            }

            if (min == null || comparator.compare(meta.minKey, min) < 0) {
                min = meta.minKey;
            }
        }
        return min;
    }

    public static StoreKey max(List<SstFileMeta> files, Comparator<RowData> userComparator) {
        StoreKeyComparator comparator = new StoreKeyComparator(userComparator);
        StoreKey max = null;
        for (SstFileMeta meta : files) {
            // TODO remove null check, comparator should deal with it if there is null key.
            if (meta.maxKey == null) {
                continue;
            }

            if (max == null || comparator.compare(meta.maxKey, max) > 0) {
                max = meta.maxKey;
            }
        }
        return max;
    }

    @Override
    public String toString() {
        return "SstFileMeta{" + "name='" + name + '\'' + '}';
    }
}
