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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.apache.flink.util.Preconditions.checkArgument;

/** */
public class Level {

    private final int level;

    private final SortedSet<SstFileMeta> levelFiles;

    private final List<SstFileMeta> addFiles;

    private final List<SstFileMeta> deleteFiles;

    private long totalSize;

    public Level(int level, List<SstFileMeta> restoreFiles, StoreKeyComparator comparator) {
        this.level = level;
        Comparator<SstFileMeta> fileComparator =
                Comparator.comparing(SstFileMeta::getMaxSequenceNumber)
                        .thenComparing(SstFileMeta::getName);
        if (level > 0) {
            fileComparator =
                    ((Comparator<SstFileMeta>)
                                    (o1, o2) -> comparator.compare(o1.getMinKey(), o2.getMinKey()))
                            .thenComparing(fileComparator);
        }
        this.levelFiles = new TreeSet<>(fileComparator);
        this.addFiles = new ArrayList<>();
        this.deleteFiles = new ArrayList<>();
        addFiles(restoreFiles);
    }

    private void addAndSumSize(List<SstFileMeta> add) {
        for (SstFileMeta file : add) {
            checkArgument(
                    file.getLevel() == level,
                    "cannot add file of level-{} to level-{}.",
                    file.getLevel(),
                    level);
            this.levelFiles.add(file);
            this.totalSize += file.getFileSize();
        }
    }

    public long totalSize() {
        return totalSize;
    }

    public SortedSet<SstFileMeta> files() {
        return Collections.unmodifiableSortedSet(levelFiles);
    }

    public void addFiles(List<SstFileMeta> add) {
        addAndSumSize(add);
        addFiles.addAll(add);
    }

    /** Return useless files. */
    public List<SstFileMeta> deleteFiles(List<SstFileMeta> delete) {
        // TODO throw exception if not find exist file

        LinkedHashMap<String, SstFileMeta> deleteMap = new LinkedHashMap<>();
        delete.forEach(file -> deleteMap.put(file.getName(), file));

        // 1. maintain data files
        Iterator<SstFileMeta> fileIter = levelFiles.iterator();
        while (fileIter.hasNext()) {
            SstFileMeta file = fileIter.next();
            String name = file.getName();
            if (deleteMap.containsKey(name)) {
                fileIter.remove();
                totalSize -= file.getFileSize();
            }
        }

        // 2. check useless add files
        List<SstFileMeta> uselessFiles = new ArrayList<>();
        Iterator<SstFileMeta> addIter = addFiles.iterator();
        while (addIter.hasNext()) {
            SstFileMeta file = addIter.next();
            String name = file.getName();
            if (deleteMap.containsKey(name)) {
                addIter.remove();
                deleteMap.remove(name);
                uselessFiles.add(file);
            }
        }

        deleteFiles.addAll(deleteMap.values());
        return uselessFiles;
    }

    public void snapshot(List<SstFileMeta> addFiles, List<SstFileMeta> deleteFiles) {
        addFiles.addAll(this.addFiles);
        this.addFiles.clear();

        deleteFiles.addAll(this.deleteFiles);
        this.deleteFiles.clear();
    }
}
