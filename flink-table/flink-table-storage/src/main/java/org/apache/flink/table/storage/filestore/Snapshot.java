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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

/** */
public class Snapshot {

    public static final String FILE_PREFIX = "SNAPSHOT-";

    private long id;

    private String manifestsFile;

    private long timestampMills;

    private Map<Integer, Long> logOffsets;

    private Map<String, String> properties;

    public Snapshot() {}

    public Snapshot(
            long id,
            String manifestsFile,
            long timestampMills,
            Map<Integer, Long> logOffsets,
            Map<String, String> properties) {
        this.id = id;
        this.manifestsFile = manifestsFile;
        this.timestampMills = timestampMills;
        this.logOffsets = logOffsets;
        this.properties = properties;
    }

    // --------------- GETTERS ---------------

    public long getId() {
        return id;
    }

    public String getManifestsFile() {
        return manifestsFile;
    }

    public long getTimestampMills() {
        return timestampMills;
    }

    public Map<Integer, Long> getLogOffsets() {
        return logOffsets;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    // --------------- SETTERS ---------------

    public void setId(long id) {
        this.id = id;
    }

    public void setManifestsFile(String manifestsFile) {
        this.manifestsFile = manifestsFile;
    }

    public void setTimestampMills(long timestampMills) {
        this.timestampMills = timestampMills;
    }

    public void setLogOffsets(Map<Integer, Long> logOffsets) {
        this.logOffsets = logOffsets;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    // --------------- HELPERS ---------------

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

    public String toJson() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static Snapshot fromJson(String json) {
        try {
            return new ObjectMapper().readValue(json, Snapshot.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
