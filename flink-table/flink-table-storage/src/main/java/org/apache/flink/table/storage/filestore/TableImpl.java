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

package org.apache.flink.table.storage.filestore;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.storage.filestore.lsm.StoreException;
import org.apache.flink.table.storage.filestore.manifest.ManifestEntry;
import org.apache.flink.table.storage.filestore.manifest.ManifestFileMeta;
import org.apache.flink.table.storage.filestore.manifest.ManifestFileReader;
import org.apache.flink.table.storage.filestore.manifest.ManifestFileWriter;
import org.apache.flink.table.storage.filestore.manifest.ManifestsFileReader;
import org.apache.flink.table.storage.filestore.manifest.ManifestsFileWriter;
import org.apache.flink.table.storage.filestore.operation.Commit;
import org.apache.flink.table.storage.filestore.operation.Expire;
import org.apache.flink.table.storage.filestore.operation.Scan;
import org.apache.flink.table.storage.filestore.utils.FileFactory;
import org.apache.flink.table.storage.filestore.utils.FileUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.storage.filestore.utils.FileFactory.SNAPSHOT_DIR;
import static org.apache.flink.table.storage.filestore.utils.FileFactory.manifestPath;
import static org.apache.flink.table.storage.filestore.utils.FileFactory.snapshotPath;

/** */
public class TableImpl implements Table {

    private final int snapshotExpireTrigger;
    private final int snapshotRetained;
    private final long maxManifestFileSize;
    private final Path basePath;
    private final ManifestFileWriter manifestWriter;
    private final ManifestFileReader manifestReader;
    private final ManifestsFileWriter manifestsWriter;
    private final ManifestsFileReader manifestsReader;
    private final FileFactory snapshotFileFactory;

    public TableImpl(
            int snapshotExpireTrigger,
            int snapshotRetained,
            long maxManifestFileSize,
            Path basePath,
            ManifestFileWriter manifestWriter,
            ManifestFileReader manifestReader,
            ManifestsFileWriter manifestsWriter,
            ManifestsFileReader manifestsReader,
            FileFactory snapshotFileFactory) {
        this.snapshotExpireTrigger = snapshotExpireTrigger;
        this.snapshotRetained = snapshotRetained;
        this.maxManifestFileSize = maxManifestFileSize;
        this.basePath = basePath;
        this.manifestWriter = manifestWriter;
        this.manifestReader = manifestReader;
        this.manifestsWriter = manifestsWriter;
        this.manifestsReader = manifestsReader;
        this.snapshotFileFactory = snapshotFileFactory;
    }

    @Override
    public Path path() {
        return basePath;
    }

    public List<ManifestFileMeta> loadManifests(Snapshot snapshot) {
        try {
            return manifestsReader.read(manifestPath(basePath, snapshot.getManifestsFile()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Scan newScan() {
        return new Scan(this);
    }

    @Override
    public Commit newCommit() {
        return new Commit(this);
    }

    @Override
    public Expire newExpire() {
        return new Expire(this);
    }

    @Override
    public List<Snapshot> loadSnapshots() {
        FileStatus[] statuses;
        try {
            FileSystem fileSystem = basePath.getFileSystem();
            statuses = fileSystem.listStatus(new Path(basePath, SNAPSHOT_DIR));
            if (statuses == null) {
                return new ArrayList<>();
            }
        } catch (IOException e) {
            throw new StoreException(e);
        }

        return Arrays.stream(statuses)
                .map(
                        file -> {
                            Long snapshotId = Snapshot.snapshotId(file.getPath().getName());
                            if (snapshotId == null) {
                                return null;
                            } else {
                                try {
                                    return loadSnapshot(snapshotId);
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            }
                        })
                .filter(Objects::nonNull)
                .sorted(Comparator.comparingLong(Snapshot::getId))
                .collect(Collectors.toList());
    }

    public Snapshot loadSnapshot(long id) throws IOException {
        return Snapshot.fromJson(FileUtils.readFileUtf8(snapshotPath(basePath, id)));
    }

    public Path writeSnapshot(Snapshot snapshot) throws IOException {
        Path file = snapshotFileFactory.newFile();
        FileUtils.writeFileUtf8(file, snapshot.toJson());
        return file;
    }

    public ManifestFileMeta writeManifest(List<ManifestEntry> files) throws IOException {
        return manifestWriter.write(files);
    }

    public Path writeManifests(List<ManifestFileMeta> manifests) throws IOException {
        return manifestsWriter.write(manifests);
    }

    public List<ManifestEntry> loadFiles(ManifestFileMeta manifest) {
        try {
            return manifestReader.read(manifestPath(basePath, manifest.getName()));
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    public int snapshotExpireTrigger() {
        return snapshotExpireTrigger;
    }

    public int snapshotRetained() {
        return snapshotRetained;
    }

    public long maxManifestFileSize() {
        return maxManifestFileSize;
    }
}
