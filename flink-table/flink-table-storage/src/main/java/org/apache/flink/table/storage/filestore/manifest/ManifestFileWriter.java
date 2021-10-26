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

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.filestore.utils.FileFactory;
import org.apache.flink.table.storage.filestore.utils.FileUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/** */
public class ManifestFileWriter implements Serializable {

    private final BulkWriter.Factory<RowData> writerFactory;

    private final FileFactory fileFactory;

    public ManifestFileWriter(BulkWriter.Factory<RowData> writerFactory, FileFactory fileFactory) {
        this.writerFactory = writerFactory;
        this.fileFactory = fileFactory;
    }

    public ManifestFileMeta write(List<ManifestEntry> orderedEntries) throws IOException {
        Path file = fileFactory.newFile();
        if (file.getFileSystem().exists(file)) {
            throw new RuntimeException("Bug???");
        }

        FSDataOutputStream out =
                file.getFileSystem().create(file, FileSystem.WriteMode.NO_OVERWRITE);
        BulkWriter<RowData> writer = writerFactory.create(out);

        long addedFilesCount = 0;
        long deletedFilesCount = 0;
        String lowerPartition = null;
        String upperPartition = null;

        for (ManifestEntry entry : orderedEntries) {
            writer.addElement(entry.toRow());

            switch (entry.kind()) {
                case ADD:
                    addedFilesCount++;
                    break;
                case DELETE:
                    deletedFilesCount++;
                    break;
            }

            if (entry.partition() != null) {
                if (lowerPartition == null || entry.partition().compareTo(lowerPartition) > 0) {
                    lowerPartition = entry.partition();
                }

                if (upperPartition == null || entry.partition().compareTo(upperPartition) < 0) {
                    upperPartition = entry.partition();
                }
            }
        }

        writer.finish();

        return new ManifestFileMeta(
                file.getName(),
                FileUtils.fileSize(file),
                addedFilesCount,
                deletedFilesCount,
                lowerPartition,
                upperPartition);
    }
}
