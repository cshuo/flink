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

package org.apache.flink.table.storage.file.lsm;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.file.lsm.compaction.CompactStrategy;
import org.apache.flink.table.storage.file.lsm.compaction.CompactionUnit;
import org.apache.flink.table.storage.file.lsm.compaction.UniversalCompaction;
import org.apache.flink.table.storage.file.lsm.merge.MergePolicy;
import org.apache.flink.table.storage.file.lsm.sst.SstFileMeta;
import org.apache.flink.table.storage.file.lsm.sst.SstFileReader;
import org.apache.flink.table.storage.file.lsm.sst.SstFileWriter;
import org.apache.flink.table.storage.file.utils.AdvanceIterator;
import org.apache.flink.table.storage.file.utils.DualIterator;
import org.apache.flink.table.storage.file.utils.FileFactory;
import org.apache.flink.table.storage.file.utils.SortMergeIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.table.storage.file.lsm.Overlap.unfoldSections;

/** */
public class FileStoreImpl implements FileStore {

    private final StoreOptions options;
    private final Path storeDir;
    private final int keyArity;
    private final int valueArity;
    private final TypeSerializer<RowData> keySerializer;
    private final TypeSerializer<RowData> valueSerializer;
    private final Comparator<RowData> keyComparator;
    private final BulkWriter.Factory<RowData> writerFactory;
    private final BulkFormat<RowData, FileSourceSplit> readerFactory;
    private final FileFactory nameFactory;
    private final MemTable memTable;
    private final MergePolicy mergePolicy;
    private final Levels levels;
    private final CompactStrategy compactStrategy;

    public FileStoreImpl(
            StoreOptions options,
            Path storeDir,
            int keyArity,
            int valueArity,
            TypeSerializer<RowData> keySerializer,
            TypeSerializer<RowData> valueSerializer,
            Comparator<RowData> keyComparator,
            BulkWriter.Factory<RowData> writerFactory,
            BulkFormat<RowData, FileSourceSplit> readerFactory,
            String fileExtension,
            MergePolicy mergePolicy,
            List<SstFileMeta> files) {
        this.options = options;
        this.storeDir = storeDir;
        this.keyArity = keyArity;
        this.valueArity = valueArity;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.keyComparator = keyComparator;
        this.writerFactory = writerFactory;
        this.readerFactory = readerFactory;
        this.memTable = new HeapMemTable(keyComparator);
        this.mergePolicy = mergePolicy;
        this.nameFactory =
                new FileFactory(storeDir, "sst", UUID.randomUUID().toString(), fileExtension);
        this.levels = new Levels(options.numLevels, files, new StoreKeyComparator(keyComparator));
        this.compactStrategy =
                new UniversalCompaction(
                        options.maxSizeAmplificationPercent,
                        options.sizeRatio,
                        options.numFilesLevel0);
    }

    @Override
    public void put(RowData key, RowData value) throws StoreException {
        memTable.put(levels.newSequenceNumber(), ValueKind.ADD, key, value);
        checkFlush();
    }

    @Override
    public void delete(RowData key, RowData value) throws StoreException {
        memTable.put(levels.newSequenceNumber(), ValueKind.DELETE, key, value);
        checkFlush();
    }

    private void checkFlush() {
        if (memTable.size() > options.maxMemRecords) {
            try {
                flush();
            } catch (IOException e) {
                throw new StoreException(e);
            }
        }
    }

    private List<SstFileMeta> writeFile(AdvanceIterator<KeyValue> orderedIter, int level)
            throws IOException {
        SstFileWriter writer =
                new SstFileWriter(
                        writerFactory, nameFactory, options.targetFileSize, keySerializer);
        return writer.write(orderedIter, level, level != 0);
    }

    private void flush() throws IOException {
        if (memTable.size() > 0) {
            try (DualIterator<KeyValue> iterator = memTable.iterator()) {
                levels.addFiles(writeFile(mergePolicy.merge(iterator, keyComparator), 0));
                memTable.clear();
            }
        }

        CompactionUnit compactionUnit = compactStrategy.pick(levels.levels());
        if (compactionUnit != null) {
            // TODO async compaction
            compact(compactionUnit);
        }
    }

    private void compact(CompactionUnit unit) throws IOException {
        Overlap overlap = new Overlap(keyComparator, unit.files());
        List<Overlap.Section> overlapped = new ArrayList<>();
        List<SstFileMeta> nonOverlapped = new ArrayList<>();
        if (unit.outputLevel() == 0) {
            overlapped.addAll(overlap.sections());
        } else {
            overlap.splitOverlapped(
                    overlapped, nonOverlapped, f -> f.getFileSize() > options.minFileSize);
        }

        List<SstFileMeta> compacted = doCompact(unit.outputLevel(), overlapped);
        levels.addFiles(compacted);
        List<SstFileMeta> uselessFiles = levels.deleteFiles(unfoldSections(overlapped));

        levels.upgrade(nonOverlapped, unit.outputLevel());

        // delete useless files in this snapshot
        for (SstFileMeta fileMeta : uselessFiles) {
            Path file = new Path(storeDir, fileMeta.getName());
            file.getFileSystem().delete(file, false);
        }
    }

    private List<SstFileMeta> doCompact(int level, List<Overlap.Section> sections)
            throws IOException {
        try (AdvanceIterator<KeyValue> iterator = sectionsIterator(sections)) {
            return writeFile(iterator, level);
        }
    }

    @Override
    public void snapshot(List<SstFileMeta> addFiles, List<SstFileMeta> deleteFiles)
            throws StoreException {
        try {
            flush();
            levels.snapshot(addFiles, deleteFiles);
        } catch (IOException e) {
            throw new StoreException(e);
        }
    }

    private AdvanceIterator<KeyValue> sectionsIterator(List<Overlap.Section> sections)
            throws IOException {
        List<Supplier<AdvanceIterator<KeyValue>>> suppliers =
                sections.stream()
                        .map(s -> (Supplier<AdvanceIterator<KeyValue>>) () -> sectionIterator(s))
                        .collect(Collectors.toList());
        return suppliers.size() == 1 ? suppliers.get(0).get() : new ConcatenatedIterator(suppliers);
    }

    private AdvanceIterator<KeyValue> sectionIterator(Overlap.Section section) {
        return merge(section.files().stream().map(this::fileIterator).collect(Collectors.toList()));
    }

    private DualIterator<KeyValue> fileIterator(SstFileMeta file) {
        SstFileReader reader =
                new SstFileReader(
                        readerFactory, keyArity, valueArity, keySerializer, valueSerializer);
        try {
            return reader.read(new Path(storeDir, file.getName()));
        } catch (IOException e) {
            throw new StoreException(e);
        }
    }

    private AdvanceIterator<KeyValue> merge(List<DualIterator<KeyValue>> iterators) {
        if (iterators.size() == 1) {
            return iterators.get(0);
        }

        return mergePolicy.merge(
                new SortMergeIterator<>(
                        iterators,
                        (o1, o2) -> {
                            int result = keyComparator.compare(o1.key(), o2.key());
                            if (result != 0) {
                                return result;
                            }

                            return Long.compare(o1.sequenceNumber(), o2.sequenceNumber());
                        }),
                keyComparator);
    }

    @Override
    public KeyValueIterator<RowData, RowData> iterator() {
        try {
            if (memTable.size() > 0) {
                throw new UnsupportedOperationException();
            }

            Overlap overlap = new Overlap(keyComparator, levels.files());
            return new UserKeyValueIterator(sectionsIterator(overlap.sections()));
        } catch (IOException e) {
            throw new StoreException(e);
        }
    }
}
