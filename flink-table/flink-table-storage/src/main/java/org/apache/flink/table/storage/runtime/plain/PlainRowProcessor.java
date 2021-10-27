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

package org.apache.flink.table.storage.runtime.plain;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.storage.filestore.lsm.merge.MergePolicy;
import org.apache.flink.table.storage.runtime.Processor;
import org.apache.flink.table.storage.runtime.RowReader;
import org.apache.flink.table.storage.runtime.RowWriter;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;

/** */
public class PlainRowProcessor implements Processor {

    private final RowType keyType;
    private final RowType valueType;
    private final GeneratedRecordComparator comparator;

    public PlainRowProcessor(RowType rowType, ComparatorFn comparatorFn) {
        this.keyType = rowType;
        this.valueType =
                new RowType(
                        Collections.singletonList(
                                new RowType.RowField("COUNT", new BigIntType(false))));
        this.comparator = comparatorFn.create(rowType);
    }

    @Override
    public ChangelogMode logChangelogMode() {
        return ChangelogMode.all();
    }

    @Override
    public RowReader createRowReader() {
        return new PlainRowReader();
    }

    @Override
    public MergePolicy compactStrategy() {
        return MergePolicy.COUNT;
    }

    @Override
    public RowWriter createRowWriter(int numBucket) {
        return new PlainRowWriter(keySerializer(), numBucket);
    }

    @Override
    public RowType keyType() {
        return keyType;
    }

    @Override
    public RowType valueType() {
        return valueType;
    }

    @Override
    public RowDataSerializer keySerializer() {
        return InternalSerializers.create(keyType);
    }

    @Override
    public RowDataSerializer valueSerializer() {
        return InternalSerializers.create(valueType);
    }

    @Override
    public GeneratedRecordComparator comparator() {
        return comparator;
    }
}
