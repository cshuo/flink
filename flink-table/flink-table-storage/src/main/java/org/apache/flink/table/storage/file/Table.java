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

package org.apache.flink.table.storage.file;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.storage.file.filter.Filter;
import org.apache.flink.table.storage.file.filter.InFilter;
import org.apache.flink.table.storage.file.lsm.StoreException;
import org.apache.flink.table.storage.file.lsm.sst.SstFileMeta;
import org.apache.flink.table.storage.file.manifest.FileIdentifier;
import org.apache.flink.table.storage.file.manifest.ManifestEntry;
import org.apache.flink.table.storage.file.manifest.ManifestFileMeta;
import org.apache.flink.table.storage.file.manifest.ManifestFileReader;
import org.apache.flink.table.storage.file.manifest.ManifestFileWriter;
import org.apache.flink.table.storage.file.manifest.ManifestMerge;
import org.apache.flink.table.storage.file.manifest.ManifestsFileReader;
import org.apache.flink.table.storage.file.manifest.ManifestsFileWriter;
import org.apache.flink.table.storage.file.snapshot.Snapshot;
import org.apache.flink.table.storage.file.snapshot.SnapshotExpire;
import org.apache.flink.table.storage.file.utils.FileFactory;
import org.apache.flink.table.storage.file.utils.FileUtils;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.storage.file.utils.FileFactory.SNAPSHOT_DIR;
import static org.apache.flink.table.storage.file.utils.FileFactory.manifestPath;
import static org.apache.flink.table.storage.file.utils.FileFactory.snapshotPath;

/** */
public class Table implements Serializable {

    private final int snapshotExpireTrigger;
    private final int snapshotRetained;
    private final long maxManifestFileSize;
    private final Path basePath;
    private final ManifestFileWriter manifestWriter;
    private final ManifestFileReader manifestReader;
    private final ManifestsFileWriter manifestsWriter;
    private final ManifestsFileReader manifestsReader;
    private final FileFactory snapshotFileFactory;

    private transient List<Snapshot> snapshots;

    public Table(
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

    public Path path() {
        return basePath;
    }

    public void commitChanges(List<ManifestEntry> newFiles, Map<Integer, Long> logOffsets)
            throws IOException {
        while (true) {
            if (tryCommit(newFiles, logOffsets)) {
                return;
            } else {
                // TODO fail when check delete files?
                reload();
            }
        }
    }

    public void expire() throws IOException {
        new SnapshotExpire(this).expire(snapshotExpireTrigger, snapshotRetained);
    }

    public PartitionFiles readLatest(String partition) throws IOException {
        PartitionFiles files =
                partition == null
                        ? readLatest().partitionFiles(null)
                        : readLatest(Collections.singletonList(partition))
                                .partitionFiles(partition);
        return files == null ? new PartitionFiles(partition, new HashMap<>()) : files;
    }

    public TableFiles readLatest() throws IOException {
        return readLatest(Filter.createTrue());
    }

    public TableFiles readLatest(List<String> partitions) throws IOException {
        return readLatest(InFilter.inPartition(partitions));
    }

    public TableFiles readLatest(Filter<String> partFilter) throws IOException {
        Snapshot snapshot = lastSnapshot();
        if (snapshot == null) {
            return TableFiles.empty();
        }

        return readSnapshot(snapshots().get(snapshots().size() - 1), partFilter);
    }

    public List<Snapshot> snapshots() throws IOException {
        if (snapshots == null) {
            reload();
        }
        return snapshots;
    }

    public TableFiles readSnapshot(Snapshot snapshot) {
        return readSnapshot(snapshot, Filter.createTrue());
    }

    private TableFiles readSnapshot(Snapshot snapshot, Filter<String> partFilter) {
        return readSnapshot(manifestsOfSnapshot(snapshot), partFilter);
    }

    public TableFiles readSnapshot(List<ManifestFileMeta> manifests) {
        return readSnapshot(manifests, Filter.createTrue());
    }

    public TableFiles readSnapshot(List<ManifestFileMeta> manifests, Filter<String> partFilter) {
        Map<String, List<ManifestEntry>> groupByPartition = new HashMap<>();
        manifests
                .parallelStream()
                .filter(e -> partFilter.test(e.getLowerPartition(), e.getUpperPartition()))
                .flatMap(e -> filesOfManifest(e).stream())
                .filter(e -> partFilter.test(e.partition()))
                .sequential()
                .forEach(
                        fileEntry ->
                                groupByPartition
                                        .computeIfAbsent(
                                                fileEntry.partition(), k -> new ArrayList<>())
                                        .add(fileEntry));

        return new TableFiles(
                groupByPartition.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        e ->
                                                new PartitionFiles(
                                                        e.getKey(), groupByBucket(e.getValue())))));
    }

    public List<ManifestFileMeta> manifestsOfSnapshot(Snapshot snapshot) {
        try {
            return manifestsReader.read(manifestPath(basePath, snapshot.getManifestsFile()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Set<String> allPartitions() throws IOException {
        Snapshot snapshot = lastSnapshot();
        if (snapshot == null) {
            return Collections.emptySet();
        }

        List<ManifestFileMeta> manifests =
                manifestsReader.read(manifestPath(basePath, snapshot.getManifestsFile()));
        return manifests
                .parallelStream()
                .flatMap(e -> filesOfManifest(e).stream().parallel())
                .map(ManifestEntry::partition)
                .collect(Collectors.toSet());
    }

    public void reload() throws IOException {
        FileSystem fileSystem = basePath.getFileSystem();
        FileStatus[] statuses = fileSystem.listStatus(new Path(basePath, SNAPSHOT_DIR));
        if (statuses == null) {
            this.snapshots = new ArrayList<>();
            return;
        }

        this.snapshots =
                Arrays.stream(statuses)
                        .map(
                                file -> {
                                    Long snapshotId = Snapshot.snapshotId(file.getPath().getName());
                                    if (snapshotId == null) {
                                        return null;
                                    } else {
                                        try {
                                            return readSnapshotFile(snapshotId);
                                        } catch (IOException e) {
                                            throw new UncheckedIOException(e);
                                        }
                                    }
                                })
                        .filter(Objects::nonNull)
                        .sorted(Comparator.comparingLong(Snapshot::getId))
                        .collect(Collectors.toList());
    }

    private Snapshot lastSnapshot() throws IOException {
        List<Snapshot> snapshots = snapshots();
        if (snapshots.isEmpty()) {
            return null;
        }
        return snapshots.get(snapshots.size() - 1);
    }

    private boolean tryCommit(List<ManifestEntry> newFiles, Map<Integer, Long> logOffsets)
            throws IOException {
        // 1. get all manifests
        Snapshot last = lastSnapshot();
        long snapshotId = last == null ? 0 : last.getId() + 1;
        List<ManifestFileMeta> lastManifests =
                last == null
                        ? Collections.emptyList()
                        : manifestsReader.read(manifestPath(basePath, last.getManifestsFile()));
        ManifestFileMeta newManifest = writeManifestFile(newFiles);
        List<ManifestFileMeta> manifests = new ArrayList<>(lastManifests);
        manifests.add(newManifest);

        // 2. merge manifests
        List<ManifestFileMeta> newManifests = new ArrayList<>();
        List<ManifestFileMeta> merged = new ManifestMerge(this).merge(manifests, newManifests);
        if (merged.contains(newManifest)) {
            newManifests.add(newManifest);
        } else {
            new SnapshotExpire(this).deleteManifest(newManifest);
        }

        // 3. write snapshot file and commit
        Path manifestsFile = manifestsWriter.write(merged);
        Snapshot snapshot =
                new Snapshot(
                        snapshotId,
                        manifestsFile.getName(),
                        System.currentTimeMillis(),
                        logOffsets,
                        new HashMap<>());

        Path tmp = writeSnapshotFile(snapshot);
        Path dst = snapshotPath(basePath, snapshotId);

        FileSystem fileSystem = tmp.getFileSystem();
        // TODO rename only works in HDFS...
        if (!fileSystem.rename(tmp, dst)) {
            // failed. we need to clear new files
            fileSystem.delete(tmp, true);
            fileSystem.delete(manifestsFile, true);
            for (ManifestFileMeta manifest : newManifests) {
                new SnapshotExpire(this).deleteManifest(manifest);
            }
            return false;
        } else {
            snapshots().add(snapshot);
            return true;
        }
    }

    private Snapshot readSnapshotFile(long id) throws IOException {
        return Snapshot.fromJson(FileUtils.readFileUtf8(snapshotPath(basePath, id)));
    }

    private Path writeSnapshotFile(Snapshot snapshot) throws IOException {
        Path file = snapshotFileFactory.newFile();
        FileUtils.writeFileUtf8(file, snapshot.toJson());
        return file;
    }

    public ManifestFileMeta writeManifestFile(List<ManifestEntry> files) throws IOException {
        return manifestWriter.write(files);
    }

    public List<ManifestEntry> filesOfManifest(ManifestFileMeta manifest) {
        try {
            return manifestReader.read(manifestPath(basePath, manifest.getName()));
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    private Map<Integer, List<SstFileMeta>> groupByBucket(List<ManifestEntry> files) {
        return files.stream()
                .collect(Collectors.groupingBy(ManifestEntry::bucket))
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> mergeFiles(e.getValue())));
    }

    private List<SstFileMeta> mergeFiles(List<ManifestEntry> files) {
        LinkedHashMap<String, SstFileMeta> fileMap = new LinkedHashMap<>();
        files.forEach(
                f -> {
                    switch (f.kind()) {
                        case ADD:
                            fileMap.put(f.name(), f.file());
                            break;
                        case DELETE:
                            SstFileMeta value = fileMap.remove(f.name());
                            if (value == null) {
                                throw new StoreException("Can not remove file: " + f.name());
                            }
                            break;
                    }
                });
        return new ArrayList<>(fileMap.values());
    }

    /** */
    public static class PartitionFiles {

        private final Map<Integer, List<SstFileMeta>> bucketsFiles;
        private final String partition;

        public PartitionFiles(String partition, Map<Integer, List<SstFileMeta>> bucketsFiles) {
            this.partition = partition;
            this.bucketsFiles = bucketsFiles;
        }

        public String partition() {
            return partition;
        }

        public List<SstFileMeta> bucketFiles(int bucket) {
            return bucketsFiles.get(bucket);
        }

        public Set<Integer> buckets() {
            return bucketsFiles.keySet();
        }

        public Set<FileIdentifier> toIdentifierSet() {
            Set<FileIdentifier> set = new HashSet<>();
            bucketsFiles.forEach(
                    (bucket, files) ->
                            files.forEach(
                                    file ->
                                            set.add(
                                                    new FileIdentifier(
                                                            partition, bucket, file.getName()))));
            return set;
        }
    }

    /** */
    public static class TableFiles {

        private final Map<String, PartitionFiles> partitions;

        public TableFiles(Map<String, PartitionFiles> partitions) {
            this.partitions = partitions;
        }

        public PartitionFiles partitionFiles(String partition) {
            return partitions.getOrDefault(
                    partition, new PartitionFiles(partition, Collections.emptyMap()));
        }

        public Set<String> partitions() {
            return partitions.keySet();
        }

        public Set<FileIdentifier> toIdentifierSet() {
            Set<FileIdentifier> set = new HashSet<>();
            partitions.forEach((partition, pFiles) -> set.addAll(pFiles.toIdentifierSet()));
            return set;
        }

        public static TableFiles empty() {
            return new TableFiles(new HashMap<>());
        }
    }

    public long maxManifestFileSize() {
        return maxManifestFileSize;
    }
}
