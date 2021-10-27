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

package org.apache.flink.table.storage.runtime;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.storage.filestore.lsm.merge.MergePolicy;
import org.apache.flink.table.storage.runtime.plain.PlainRowProcessor;
import org.apache.flink.table.storage.runtime.primarykey.PrimaryKeyProcessor;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/** */
public interface Processor {

    ChangelogMode logChangelogMode();

    RowReader createRowReader();

    MergePolicy compactStrategy();

    RowWriter createRowWriter(int numBucket);

    RowType keyType();

    RowType valueType();

    RowDataSerializer keySerializer();

    RowDataSerializer valueSerializer();

    GeneratedRecordComparator comparator();

    static Processor create(ResolvedSchema schema) {
        return Processor.create(
                createKeySelector(schema),
                (RowType) schema.toPhysicalRowDataType().getLogicalType(),
                type ->
                        ComparatorCodeGenerator.gen(
                                new TableConfig(),
                                "KeyComparator",
                                type,
                                SortSpec.defaultSortAll(type.getFieldCount())));
    }

    static Processor create(
            RowDataKeySelector keySelector, RowType rowType, ComparatorFn comparatorFn) {
        if (keySelector != null) {
            return new PrimaryKeyProcessor(keySelector, rowType, comparatorFn);
        } else {
            return new PlainRowProcessor(rowType, comparatorFn);
        }
    }

    static RowDataKeySelector createKeySelector(ResolvedSchema schema) {
        RowType rowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();
        List<String> fieldNames = rowType.getFieldNames();
        return schema.getPrimaryKey()
                .map(k -> k.getColumns().stream().mapToInt(fieldNames::indexOf).toArray())
                .map(keys -> KeySelectorUtil.getRowDataSelector(keys, InternalTypeInfo.of(rowType)))
                .orElse(null);
    }

    /** */
    @FunctionalInterface
    interface ComparatorFn {
        GeneratedRecordComparator create(RowType type);
    }
}
