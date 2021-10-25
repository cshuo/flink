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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** */
public class DynamicCommittable<LogCommT> {

    private final List<LogCommT> logCommittables;
    private final Map<Integer, Long> logOffsets;

    private final List<ManifestEntry> entries;

    public DynamicCommittable() {
        this(new ArrayList<>());
    }

    public DynamicCommittable(
            List<LogCommT> logCommittables,
            Map<Integer, Long> logOffsets,
            List<ManifestEntry> entries) {
        this.logCommittables = logCommittables;
        this.logOffsets = logOffsets;
        this.entries = entries;
    }

    public DynamicCommittable(List<LogCommT> logCommittables, Map<Integer, Long> logOffsets) {
        this(logCommittables, logOffsets, new ArrayList<>());
    }

    public DynamicCommittable(List<ManifestEntry> entries) {
        this(new ArrayList<>(), new HashMap<>(), entries);
    }

    public DynamicCommittable(
            String partition,
            int bucket,
            int numBucket,
            List<SstFileMeta> addFiles,
            List<SstFileMeta> deleteFiles) {
        this(entries(partition, bucket, numBucket, addFiles, deleteFiles));
    }

    private static List<ManifestEntry> entries(
            String partition,
            int bucket,
            int numBucket,
            List<SstFileMeta> addFiles,
            List<SstFileMeta> deleteFiles) {
        List<ManifestEntry> entries = new ArrayList<>();

        // always delete files first, otherwise upgrade files will be ignored
        for (SstFileMeta file : deleteFiles) {
            entries.add(new ManifestEntry(FileKind.DELETE, partition, bucket, numBucket, file));
        }

        for (SstFileMeta file : addFiles) {
            entries.add(new ManifestEntry(FileKind.ADD, partition, bucket, numBucket, file));
        }

        return entries;
    }

    public void merge(List<DynamicCommittable<LogCommT>> committables) {
        committables.forEach(this::merge);
    }

    public void merge(DynamicCommittable<LogCommT> committable) {
        logCommittables.addAll(committable.logCommittables);
        committable.logOffsets.forEach(
                (bucket, offset) ->
                        logOffsets.compute(
                                bucket,
                                (k, v) -> {
                                    if (v != null) {
                                        return Math.max(v, offset);
                                    }
                                    return offset;
                                }));
        logOffsets.putAll(committable.logOffsets);
        entries.addAll(committable.entries);
    }

    public List<ManifestEntry> getEntries() {
        return entries;
    }

    public List<LogCommT> getLogCommittables() {
        return logCommittables;
    }

    public Map<Integer, Long> getLogOffsets() {
        return logOffsets;
    }
}
