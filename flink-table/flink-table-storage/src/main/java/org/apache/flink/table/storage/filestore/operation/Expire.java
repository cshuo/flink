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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.storage.filestore.Snapshot;
import org.apache.flink.table.storage.filestore.TableImpl;
import org.apache.flink.table.storage.filestore.manifest.FileIdentifier;
import org.apache.flink.table.storage.filestore.manifest.FileKind;
import org.apache.flink.table.storage.filestore.manifest.ManifestEntry;
import org.apache.flink.table.storage.filestore.manifest.ManifestFileMeta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.storage.filestore.utils.FileFactory.BUCKET_DIR_PREFIX;
import static org.apache.flink.table.storage.filestore.utils.FileFactory.MANIFEST_DIR;
import static org.apache.flink.table.storage.filestore.utils.FileFactory.SNAPSHOT_DIR;

/** */
public class Expire {

    private static final Logger LOG = LoggerFactory.getLogger(Expire.class);

    private final TableImpl table;

    private List<Snapshot> snapshots;

    public Expire(TableImpl table) {
        this.table = table;
    }

    public Expire withSnapshots(List<Snapshot> snapshots) {
        this.snapshots = snapshots;
        return this;
    }

    public void expire() throws IOException {
        if (snapshots == null) {
            snapshots = table.loadSnapshots();
        }
        expire(snapshots, table.snapshotExpireTrigger(), table.snapshotRetained());
    }

    private void expire(List<Snapshot> snapshots, int snapshotExpireTrigger, int snapshotRetained)
            throws IOException {
        // be deleted right away. The order doesn't matter.
        Set<ManifestFileMeta> manifests = new HashSet<>();
        if (snapshots.size() > snapshotExpireTrigger) {
            List<Snapshot> expired = new ArrayList<>();
            int earliestLive = snapshots.size() - snapshotRetained;
            for (int i = 0; i < earliestLive; i++) {
                Snapshot snapshot = snapshots.get(i);
                expired.add(snapshot);
                manifests.addAll(table.loadManifests(snapshot));
            }

            expireEarliest(manifests, snapshots.get(earliestLive));

            for (Snapshot snapshot : expired) {
                deleteSnapshot(snapshot);
            }
        }
    }

    private void expireEarliest(Set<ManifestFileMeta> manifests, Snapshot nextSnapshot)
            throws IOException {
        List<ManifestFileMeta> liveManifests = table.loadManifests(nextSnapshot);
        liveManifests.forEach(manifests::remove);

        // better way to avoid reading files of next snapshot?
        Set<FileIdentifier> liveFiles =
                table.newScan().withManifests(liveManifests).plan().stream()
                        .map(ManifestEntry::identifier)
                        .collect(Collectors.toSet());

        List<ManifestEntry> dataFiles =
                manifests
                        .parallelStream()
                        .flatMap(m -> table.loadFiles(m).stream())
                        .filter(e -> e.kind() == FileKind.ADD)
                        .collect(Collectors.toList());

        for (ManifestEntry e : dataFiles) {
            if (!liveFiles.contains(e.identifier())) {
                deleteFile(e);
                LOG.debug("delete file: {}", e.identifier());
            }
        }

        for (ManifestFileMeta m : manifests) {
            deleteManifest(table.path(), m);
        }
    }

    private void deleteFile(ManifestEntry e) throws IOException {
        Path file = e.partition() == null ? table.path() : new Path(table.path(), e.partition());
        file = new Path(new Path(file, BUCKET_DIR_PREFIX + e.bucket()), e.name());
        file.getFileSystem().delete(file, false);
    }

    private void deleteSnapshot(Snapshot snapshot) throws IOException {
        Path file = new Path(new Path(table.path(), SNAPSHOT_DIR), snapshot.fileName());
        file.getFileSystem().delete(file, false);
    }

    public static void deleteManifest(Path path, ManifestFileMeta m) throws IOException {
        Path file = new Path(new Path(path, MANIFEST_DIR), m.getName());
        file.getFileSystem().delete(file, false);
    }
}
