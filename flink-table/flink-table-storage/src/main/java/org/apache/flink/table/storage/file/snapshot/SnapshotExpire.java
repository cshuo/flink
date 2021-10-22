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

package org.apache.flink.table.storage.file.snapshot;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.storage.file.Table;
import org.apache.flink.table.storage.file.manifest.FileIdentifier;
import org.apache.flink.table.storage.file.manifest.FileKind;
import org.apache.flink.table.storage.file.manifest.ManifestEntry;
import org.apache.flink.table.storage.file.manifest.ManifestFileMeta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.storage.file.utils.FileFactory.BUCKET_DIR_PREFIX;
import static org.apache.flink.table.storage.file.utils.FileFactory.MANIFEST_DIR;
import static org.apache.flink.table.storage.file.utils.FileFactory.SNAPSHOT_DIR;

/** */
public class SnapshotExpire {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotExpire.class);

    private final Table table;

    public SnapshotExpire(Table table) {
        this.table = table;
    }

    public void expire(int snapshotExpireTrigger, int snapshotRetained) throws IOException {
        // be deleted right away. The order doesn't matter.
        Set<ManifestFileMeta> manifests = new HashSet<>();
        List<Snapshot> snapshots = table.snapshots();
        if (snapshots.size() > snapshotExpireTrigger) {
            LOG.debug("--------- start expire! -------------");
            debug(snapshots);
            List<Snapshot> expired = new ArrayList<>();
            int earliestLive = snapshots.size() - snapshotRetained;
            for (int i = 0; i < earliestLive; i++) {
                Snapshot snapshot = snapshots.get(i);
                expired.add(snapshot);
                manifests.addAll(table.manifestsOfSnapshot(snapshot));
            }

            expireEarliest(manifests, snapshots.get(earliestLive));

            for (Snapshot snapshot : expired) {
                deleteSnapshot(snapshot);
            }

            table.reload();
            LOG.debug("--------- end expire! -------------");
        }
    }

    private void debug(List<Snapshot> snapshots) {
        if (LOG.isDebugEnabled()) {
            snapshots.forEach(
                    snapshot -> {
                        LOG.debug("files of " + snapshot.fileName() + ":");
                        LOG.debug(table.readSnapshot(snapshot).toIdentifierSet().toString());
                    });
        }
    }

    private void expireEarliest(Set<ManifestFileMeta> manifests, Snapshot nextSnapshot)
            throws IOException {
        List<ManifestFileMeta> liveManifests = table.manifestsOfSnapshot(nextSnapshot);
        liveManifests.forEach(manifests::remove);

        // better way to avoid reading files of next snapshot?
        Set<FileIdentifier> liveFiles = table.readSnapshot(liveManifests).toIdentifierSet();

        List<ManifestEntry> dataFiles =
                manifests
                        .parallelStream()
                        .flatMap(m -> table.filesOfManifest(m).stream())
                        .filter(e -> e.kind() == FileKind.ADD)
                        .collect(Collectors.toList());

        for (ManifestEntry e : dataFiles) {
            if (!liveFiles.contains(e.identifier())) {
                deleteFile(e);
                LOG.debug("delete file: {}", e.identifier());
            }
        }

        for (ManifestFileMeta m : manifests) {
            deleteManifest(m);
        }
    }

    private void deleteFile(ManifestEntry e) throws IOException {
        Path file = e.partition() == null ? table.path() : new Path(table.path(), e.partition());
        file = new Path(new Path(file, BUCKET_DIR_PREFIX + e.bucket()), e.name());
        file.getFileSystem().delete(file, false);
    }

    public void deleteManifest(ManifestFileMeta m) throws IOException {
        Path file = new Path(new Path(table.path(), MANIFEST_DIR), m.getName());
        file.getFileSystem().delete(file, false);
    }

    public void deleteSnapshot(Snapshot snapshot) throws IOException {
        Path file = new Path(new Path(table.path(), SNAPSHOT_DIR), snapshot.fileName());
        file.getFileSystem().delete(file, false);
    }
}
