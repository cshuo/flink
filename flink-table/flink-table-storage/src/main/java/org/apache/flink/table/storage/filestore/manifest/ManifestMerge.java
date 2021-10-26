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

package org.apache.flink.table.storage.filestore.manifest;

import org.apache.flink.table.storage.filestore.TableImpl;
import org.apache.flink.table.storage.filestore.lsm.StoreException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/** */
public class ManifestMerge {

    private final TableImpl table;

    public ManifestMerge(TableImpl table) {
        this.table = table;
    }

    public List<ManifestFileMeta> merge(
            List<ManifestFileMeta> manifests, List<ManifestFileMeta> newManifests)
            throws IOException {
        // TODO optimize merge strategy, we can merge manifest even it reach max file size.
        // TODO the strategy should consider delete files count of next manifest.

        List<ManifestFileMeta> merged = new ArrayList<>();

        List<ManifestFileMeta> candidate = new ArrayList<>();
        long totalSize = 0;
        for (ManifestFileMeta meta : manifests) {
            totalSize += meta.getFileSize();
            candidate.add(meta);
            if (totalSize > table.maxManifestFileSize()) {
                merged.add(doMerge(candidate, newManifests));
                candidate.clear();
                totalSize = 0;
            }
        }

        if (candidate.size() > 0) {
            merged.add(doMerge(candidate, newManifests));
        }

        return merged;
    }

    private ManifestFileMeta doMerge(
            List<ManifestFileMeta> manifests, List<ManifestFileMeta> newManifests)
            throws IOException {
        if (manifests.size() == 1) {
            return manifests.get(0);
        }

        LinkedHashMap<FileIdentifier, ManifestEntry> entries = new LinkedHashMap<>();
        for (ManifestFileMeta manifest : manifests) {
            for (ManifestEntry entry : table.loadFiles(manifest)) {
                switch (entry.kind()) {
                    case ADD:
                        entries.put(entry.identifier(), entry);
                        break;
                    case DELETE:
                        ManifestEntry value = entries.remove(entry.identifier());
                        if (value == null) {
                            throw new StoreException("Can not remove file: " + entry.identifier());
                        }
                        break;
                }
            }
        }

        List<ManifestEntry> newFiles = new ArrayList<>(entries.values());
        ManifestFileMeta newManifest = table.writeManifest(newFiles);
        newManifests.add(newManifest);

        return newManifest;
    }
}
