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

package org.apache.flink.table.storage.file.manifest;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.Utils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/** */
public class ManifestFileReader implements Serializable {

    private final Configuration configuration;
    private final BulkFormat<RowData, FileSourceSplit> readerFactory;
    private final int keyArity;
    private final TypeSerializer<RowData> keySerializer;

    public ManifestFileReader(
            BulkFormat<RowData, FileSourceSplit> readerFactory,
            int keyArity,
            TypeSerializer<RowData> keySerializer) {
        this.configuration = new Configuration();
        this.readerFactory = readerFactory;
        this.keyArity = keyArity;
        this.keySerializer = keySerializer;
    }

    public List<ManifestEntry> read(Path path) throws IOException {
        List<ManifestEntry> entries = new ArrayList<>();

        FileSourceSplit split =
                new FileSourceSplit(
                        "DUMMY", path, 0, path.getFileSystem().getFileStatus(path).getLen());
        BulkFormat.Reader<RowData> reader = readerFactory.createReader(configuration, split);

        Utils.forEachRemaining(
                reader, row -> entries.add(ManifestEntry.fromRow(row, keyArity, keySerializer)));

        return entries;
    }
}
