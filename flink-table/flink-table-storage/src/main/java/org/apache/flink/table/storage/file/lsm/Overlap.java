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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.file.lsm.sst.SstFileMeta;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** */
public class Overlap {

    private final Comparator<RowData> comparator;
    private final TreeMap<RowData, Section> sectionMap;

    public Overlap(Comparator<RowData> comparator, List<SstFileMeta> files) {
        this.comparator = comparator;
        this.sectionMap = new TreeMap<>(comparator);
        for (SstFileMeta file : files) {
            Section section = new Section(file);

            while (true) {
                Map.Entry<RowData, Section> floorMax = sectionMap.floorEntry(section.max);
                if (floorMax != null
                        && comparator.compare(section.min, floorMax.getValue().max) <= 0) {
                    sectionMap.remove(floorMax.getKey());
                    section = section.merge(floorMax.getValue());
                } else {
                    sectionMap.put(section.min, section);
                    break;
                }
            }
        }
    }

    public List<Section> sections() {
        return new ArrayList<>(sectionMap.values());
    }

    public void splitOverlapped(
            List<Section> overlapped,
            List<SstFileMeta> nonOverlapped,
            Predicate<SstFileMeta> satisfy) {
        for (Section section : sectionMap.values()) {
            if (section.isSingleFile() && satisfy.test(section.files().get(0))) {
                nonOverlapped.add(section.files().get(0));
            } else {
                overlapped.add(section);
            }
        }
    }

    public static List<SstFileMeta> unfoldSections(List<Overlap.Section> sections) {
        return sections.stream()
                .map(Overlap.Section::files)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    /** */
    public class Section {

        private final RowData min;
        private final RowData max;
        private final List<SstFileMeta> files;

        private Section(SstFileMeta file) {
            this.min = file.getMinKey().getUserKey();
            this.max = file.getMaxKey().getUserKey();
            this.files = new ArrayList<>();
            this.files.add(file);
        }

        private Section(RowData min, RowData max, List<SstFileMeta> files) {
            this.min = min;
            this.max = max;
            this.files = files;
        }

        private Section merge(Section section) {
            RowData newMin = min;
            RowData newMax = max;
            if (comparator.compare(min, section.min) > 0) {
                newMin = section.min;
            }

            if (comparator.compare(max, section.max) < 0) {
                newMax = section.max;
            }

            files.addAll(section.files);
            return new Section(newMin, newMax, files);
        }

        public RowData min() {
            return min;
        }

        public RowData max() {
            return max;
        }

        public List<SstFileMeta> files() {
            return Collections.unmodifiableList(files);
        }

        public boolean isSingleFile() {
            return files.size() == 1;
        }
    }
}
