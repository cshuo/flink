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

package org.apache.flink.table.storage.filestore.operation;

import org.apache.flink.table.storage.filestore.Snapshot;
import org.apache.flink.table.storage.filestore.TableImpl;
import org.apache.flink.table.storage.filestore.filter.Filter;
import org.apache.flink.table.storage.filestore.lsm.StoreException;
import org.apache.flink.table.storage.filestore.lsm.sst.SstFileMeta;
import org.apache.flink.table.storage.filestore.manifest.FileIdentifier;
import org.apache.flink.table.storage.filestore.manifest.ManifestEntry;
import org.apache.flink.table.storage.filestore.manifest.ManifestFileMeta;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** */
public class Scan {

    private final TableImpl table;

    private Filter<String> partitionFilter = Filter.createTrue();

    private Integer bucket;

    private Long snapshotId;

    private List<ManifestFileMeta> manifests;

    public Scan(TableImpl table) {
        this.table = table;
    }

    public Scan withPartitionFilter(Filter<String> partitionFilter) {
        this.partitionFilter = partitionFilter;
        return this;
    }

    public Scan withBucket(Integer bucket) {
        this.bucket = bucket;
        return this;
    }

    public Scan withSnapshot(Long snapshotId) {
        this.snapshotId = snapshotId;
        return this;
    }

    public Scan withManifests(List<ManifestFileMeta> manifests) {
        this.manifests = manifests;
        return this;
    }

    public Plan plan() {
        if (manifests != null && snapshotId != null) {
            throw new StoreException("manifests and snapshotId cannot be specified together.");
        }

        List<ManifestFileMeta> manifests = this.manifests;

        Snapshot snapshot = null;

        if (manifests == null) {
            List<Snapshot> snapshots = table.loadSnapshots();
            if (snapshots.isEmpty()) {
                return new Plan(null, Collections.emptyList());
            }

            snapshot = snapshots.get(snapshots.size() - 1);
            if (snapshotId != null) {
                boolean find = false;
                for (Snapshot s : snapshots) {
                    if (s.getId() == snapshotId) {
                        find = true;
                        snapshot = s;
                        break;
                    }
                }
                if (!find) {
                    throw new StoreException("Can not find snapshot: " + snapshotId);
                }
            }

            manifests = table.loadManifests(snapshot);
        }

        Stream<ManifestFileMeta> filteredManifests =
                manifests
                        .parallelStream()
                        .filter(
                                manifest ->
                                        partitionFilter.test(
                                                manifest.getLowerPartition(),
                                                manifest.getUpperPartition()));

        LinkedHashMap<FileIdentifier, ManifestEntry> fileMap = new LinkedHashMap<>();
        filteredManifests
                .flatMap(manifest -> table.loadFiles(manifest).stream())
                .filter(file -> partitionFilter.test(file.partition()))
                .filter(file -> bucket == null || bucket == file.bucket())
                .forEachOrdered(
                        file -> {
                            FileIdentifier identifier = file.identifier();
                            switch (file.kind()) {
                                case ADD:
                                    fileMap.put(identifier, file);
                                    break;
                                case DELETE:
                                    ManifestEntry value = fileMap.remove(identifier);
                                    if (value == null) {
                                        throw new StoreException(
                                                "Can not remove file: " + identifier);
                                    }
                                    break;
                            }
                        });

        return new Plan(snapshot, new ArrayList<>(fileMap.values()));
    }

    public static Map<String, Map<Integer, List<SstFileMeta>>> groupByPartition(
            List<ManifestEntry> files) {
        Map<String, Map<Integer, List<SstFileMeta>>> ret = new HashMap<>();
        files.forEach(
                file ->
                        ret.computeIfAbsent(file.partition(), k -> new HashMap<>())
                                .computeIfAbsent(file.bucket(), k -> new ArrayList<>())
                                .add(file.file()));
        return ret;
    }

    /** */
    public static class Plan {

        @Nullable public final Snapshot snapshot;
        public final List<ManifestEntry> files;

        public Plan(@Nullable Snapshot snapshot, List<ManifestEntry> files) {
            this.snapshot = snapshot;
            this.files = files;
        }
    }
}
