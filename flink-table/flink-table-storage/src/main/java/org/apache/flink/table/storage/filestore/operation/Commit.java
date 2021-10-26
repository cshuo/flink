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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.storage.filestore.Snapshot;
import org.apache.flink.table.storage.filestore.TableImpl;
import org.apache.flink.table.storage.filestore.manifest.ManifestEntry;
import org.apache.flink.table.storage.filestore.manifest.ManifestFileMeta;
import org.apache.flink.table.storage.filestore.manifest.ManifestMerge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.storage.filestore.utils.FileFactory.snapshotPath;

/** */
public class Commit {

    private final TableImpl table;

    public Commit(TableImpl table) {
        this.table = table;
    }

    public List<Snapshot> commit(List<ManifestEntry> newFiles, Map<Integer, Long> logOffsets)
            throws IOException {
        while (true) {
            try {
                return tryCommit(newFiles, logOffsets);
            } catch (ConflictException e) {
                // TODO fail when check delete files?
            }
        }
    }

    private List<Snapshot> tryCommit(List<ManifestEntry> newFiles, Map<Integer, Long> logOffsets)
            throws IOException, ConflictException {
        // 1. get all manifests
        List<Snapshot> snapshots = table.loadSnapshots();
        long snapshotId = 0;
        List<ManifestFileMeta> lastManifests = Collections.emptyList();
        if (snapshots.size() > 0) {
            Snapshot last = snapshots.get(snapshots.size() - 1);
            snapshotId = last.getId() + 1;
            lastManifests = table.loadManifests(last);
        }
        ManifestFileMeta newManifest = table.writeManifest(newFiles);
        List<ManifestFileMeta> manifests = new ArrayList<>(lastManifests);
        manifests.add(newManifest);

        // 2. merge manifests
        List<ManifestFileMeta> newManifests = new ArrayList<>();
        List<ManifestFileMeta> merged = new ManifestMerge(table).merge(manifests, newManifests);
        if (merged.contains(newManifest)) {
            newManifests.add(newManifest);
        } else {
            Expire.deleteManifest(table.path(), newManifest);
        }

        // 3. write snapshot file and commit
        Path manifestsFile = table.writeManifests(merged);
        Snapshot snapshot =
                new Snapshot(
                        snapshotId,
                        manifestsFile.getName(),
                        System.currentTimeMillis(),
                        logOffsets,
                        new HashMap<>());

        Path tmp = table.writeSnapshot(snapshot);
        Path dst = snapshotPath(table.path(), snapshotId);

        FileSystem fileSystem = tmp.getFileSystem();
        // TODO rename only works in HDFS...
        // TODO get lock from catalog?
        if (!fileSystem.rename(tmp, dst)) {
            // failed. we need to clear new files
            fileSystem.delete(tmp, true);
            fileSystem.delete(manifestsFile, true);
            for (ManifestFileMeta manifest : newManifests) {
                Expire.deleteManifest(table.path(), manifest);
            }
            throw new ConflictException();
        } else {
            snapshots.add(snapshot);
            return snapshots;
        }
    }

    /** */
    public static class ConflictException extends Exception {}
}
