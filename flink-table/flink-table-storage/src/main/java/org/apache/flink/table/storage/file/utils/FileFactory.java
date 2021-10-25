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

package org.apache.flink.table.storage.file.utils;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.storage.file.snapshot.Snapshot;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

/** */
public class FileFactory implements Serializable {

    public static final String MANIFEST_DIR = "manifest";
    public static final String SNAPSHOT_DIR = "snapshot";
    public static final String BUCKET_DIR_PREFIX = "bucket-";

    private final Path dir;
    private final String prefix;
    private final String uuid;
    private final String extension;
    private final AtomicInteger fileCount;

    public FileFactory(Path dir, String prefix, String uuid, String extension) {
        this.dir = dir;
        this.prefix = prefix;
        this.uuid = uuid;
        this.extension = extension;
        this.fileCount = new AtomicInteger(0);
    }

    public Path newFile() {
        return new Path(
                dir, prefix + "-" + uuid + "-" + fileCount.incrementAndGet() + "." + extension);
    }

    public static Path manifestPath(Path basePath, String name) {
        return new Path(new Path(basePath, MANIFEST_DIR), name);
    }

    public static Path snapshotPath(Path basePath, long snapshotId) {
        return new Path(new Path(basePath, SNAPSHOT_DIR), Snapshot.fileName(snapshotId));
    }
}
