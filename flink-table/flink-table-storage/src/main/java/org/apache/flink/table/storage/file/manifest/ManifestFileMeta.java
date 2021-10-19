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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** */
public class ManifestFileMeta {

    private final String name;

    private final long fileSize;

    private final long addedFilesCount;

    private final long deletedFilesCount;

    private final String lowerPartition;

    private final String upperPartition;

    public ManifestFileMeta(
            String name,
            long fileSize,
            long addedFilesCount,
            long deletedFilesCount,
            String lowerPartition,
            String upperPartition) {
        this.name = name;
        this.fileSize = fileSize;
        this.addedFilesCount = addedFilesCount;
        this.deletedFilesCount = deletedFilesCount;
        this.lowerPartition = lowerPartition;
        this.upperPartition = upperPartition;
    }

    public String getName() {
        return name;
    }

    public long getFileSize() {
        return fileSize;
    }

    public long getAddedFilesCount() {
        return addedFilesCount;
    }

    public long getDeletedFilesCount() {
        return deletedFilesCount;
    }

    public String getLowerPartition() {
        return lowerPartition;
    }

    public String getUpperPartition() {
        return upperPartition;
    }

    public RowData toRow() {
        GenericRowData row = new GenericRowData(6);
        row.setField(0, StringData.fromString(getName()));
        row.setField(1, getFileSize());
        row.setField(2, getAddedFilesCount());
        row.setField(3, getDeletedFilesCount());
        row.setField(4, StringData.fromString(getLowerPartition()));
        row.setField(5, StringData.fromString(getUpperPartition()));
        return row;
    }

    public static RowType schema() {
        List<RowType.RowField> fields = new ArrayList<>();
        fields.add(new RowType.RowField("_FILE_NAME", new VarCharType(false, Integer.MAX_VALUE)));
        fields.add(new RowType.RowField("_FILE_SIZE", new BigIntType(false)));
        fields.add(new RowType.RowField("_ADDED_FILES_COUNT", new BigIntType(false)));
        fields.add(new RowType.RowField("_DELETED_FILES_COUNT", new BigIntType(false)));
        fields.add(new RowType.RowField("_LOWER_PARTITION", new VarCharType(Integer.MAX_VALUE)));
        fields.add(new RowType.RowField("_UPPER_PARTITION", new VarCharType(Integer.MAX_VALUE)));
        return new RowType(fields);
    }

    public static ManifestFileMeta fromRow(RowData row) {
        return new ManifestFileMeta(
                row.getString(0).toString(),
                row.getLong(1),
                row.getLong(2),
                row.getLong(3),
                row.isNullAt(4) ? null : row.getString(4).toString(),
                row.isNullAt(5) ? null : row.getString(5).toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ManifestFileMeta that = (ManifestFileMeta) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
