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

import org.apache.flink.table.storage.file.lsm.sst.SstFileMeta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** */
public class FileManager {

    private final List<Level> levels;

    private long newSequenceNumber;

    public FileManager(int numLevels, List<SstFileMeta> fileList, StoreKeyComparator comparator) {
        List<List<SstFileMeta>> levelFiles = new ArrayList<>();
        for (int i = 0; i < numLevels; i++) {
            levelFiles.add(new ArrayList<>());
        }

        for (SstFileMeta file : fileList) {
            int level = file.getLevel();
            if (level >= numLevels) {
                throw new StoreException(
                        String.format(
                                "File level-%s exceeds the maximum level-%s.",
                                level, numLevels - 1));
            }

            levelFiles.get(level).add(file);
        }

        this.levels = new ArrayList<>(levelFiles.size());
        for (int i = 0; i < numLevels; i++) {
            this.levels.add(new Level(i, levelFiles.get(i), comparator));
        }

        long maxSequenceNumber =
                fileList.stream()
                        .map(SstFileMeta::getMaxSequenceNumber)
                        .max(Long::compare)
                        .orElse(-1L);
        this.newSequenceNumber = maxSequenceNumber + 1;
    }

    public long newSequenceNumber() {
        return newSequenceNumber++;
    }

    public void addFiles(List<SstFileMeta> add) {
        groupByLevel(add).forEach((level, files) -> levels.get(level).addFiles(files));
    }

    /** Return useless files. */
    public List<SstFileMeta> deleteFiles(List<SstFileMeta> delete) {
        List<SstFileMeta> uselessFiles = new ArrayList<>();
        groupByLevel(delete)
                .forEach(
                        (level, files) ->
                                uselessFiles.addAll(levels.get(level).deleteFiles(files)));
        return uselessFiles;
    }

    public void upgrade(List<SstFileMeta> files, int newLevel) {
        for (SstFileMeta file : files) {
            if (file.getLevel() != newLevel) {
                levels.get(file.getLevel()).deleteFiles(Collections.singletonList(file));
                levels.get(newLevel).addFiles(Collections.singletonList(file.upgrade(newLevel)));
            }
        }
    }

    private Map<Integer, List<SstFileMeta>> groupByLevel(List<SstFileMeta> files) {
        return files.stream()
                .collect(Collectors.groupingBy(SstFileMeta::getLevel, Collectors.toList()));
    }

    public List<SstFileMeta> files() {
        return levels.stream()
                .map(Level::files)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    void snapshot(List<SstFileMeta> addFiles, List<SstFileMeta> deleteFiles) {
        levels.forEach(level -> level.snapshot(addFiles, deleteFiles));
    }

    public List<Level> levels() {
        return Collections.unmodifiableList(levels);
    }
}
