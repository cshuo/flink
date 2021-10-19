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

/** */
public class Snapshot {

    public static final String FILE_PREFIX = "SNAPSHOT-";

    private final long id;

    private final long timestampMills;

    public Snapshot(long id, long timestampMills) {
        this.id = id;
        this.timestampMills = timestampMills;
    }

    public long getId() {
        return id;
    }

    public long getTimestampMills() {
        return timestampMills;
    }

    public String fileName() {
        return fileName(id);
    }

    public static String fileName(long id) {
        return FILE_PREFIX + id;
    }

    public static Long snapshotId(String name) {
        if (name.startsWith(FILE_PREFIX)) {
            try {
                return Long.parseLong(name.substring(FILE_PREFIX.length()));
            } catch (NumberFormatException ignore) {
            }
        }
        return null;
    }
}
