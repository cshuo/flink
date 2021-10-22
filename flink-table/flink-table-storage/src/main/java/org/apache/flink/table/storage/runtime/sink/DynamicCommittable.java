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

import org.apache.flink.table.storage.file.lsm.sst.SstFileMeta;
import org.apache.flink.table.storage.file.manifest.FileKind;
import org.apache.flink.table.storage.file.manifest.ManifestEntry;

import java.util.ArrayList;
import java.util.List;

/** */
public class DynamicCommittable {

    private final List<ManifestEntry> entries;

    public DynamicCommittable() {
        this.entries = new ArrayList<>();
    }

    public DynamicCommittable(List<ManifestEntry> entries) {
        this.entries = entries;
    }

    public DynamicCommittable(
            String partition,
            int bucket,
            List<SstFileMeta> addFiles,
            List<SstFileMeta> deleteFiles) {
        this.entries = new ArrayList<>();

        // always delete files first, otherwise upgrade files will be ignored
        for (SstFileMeta file : deleteFiles) {
            entries.add(new ManifestEntry(FileKind.DELETE, partition, bucket, file));
        }

        for (SstFileMeta file : addFiles) {
            entries.add(new ManifestEntry(FileKind.ADD, partition, bucket, file));
        }
    }

    public void merge(List<DynamicCommittable> committables) {
        committables.forEach(this::merge);
    }

    public void merge(DynamicCommittable committable) {
        entries.addAll(committable.entries);
    }

    public List<ManifestEntry> getEntries() {
        return entries;
    }
}
