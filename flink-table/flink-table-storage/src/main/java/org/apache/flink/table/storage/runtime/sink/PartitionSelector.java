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

package org.apache.flink.table.storage.runtime.sink;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.flink.table.data.RowData.createFieldGetter;
import static org.apache.flink.table.utils.PartitionPathUtils.generatePartitionPath;

/** */
public class PartitionSelector implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final String defaultPartValue;
    protected final List<String> partitionKeys;
    protected final RowData.FieldGetter[] partitionKeyGetters;

    public PartitionSelector(RowType rowType, List<String> partitionKeys, String defaultPartValue) {
        this.defaultPartValue = defaultPartValue;
        this.partitionKeys = partitionKeys;
        this.partitionKeyGetters = new RowData.FieldGetter[partitionKeys.size()];

        List<String> fieldNames = rowType.getFieldNames();
        for (int i = 0; i < partitionKeys.size(); i++) {
            int position = fieldNames.indexOf(partitionKeys.get(i));
            partitionKeyGetters[i] = createFieldGetter(rowType.getTypeAt(position), position);
        }
    }

    public String select(RowData in) {
        if (partitionKeys.isEmpty()) {
            return null;
        }
        return generatePartitionPath(selectPartValues(in));
    }

    private LinkedHashMap<String, String> selectPartValues(RowData in) {
        LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();

        for (int i = 0; i < partitionKeyGetters.length; i++) {
            Object field = partitionKeyGetters[i].getFieldOrNull(in);
            String partitionValue = field != null ? field.toString() : null;
            if (partitionValue == null || "".equals(partitionValue)) {
                partitionValue = defaultPartValue;
            }
            partSpec.put(partitionKeys.get(i), partitionValue);
        }
        return partSpec;
    }
}
