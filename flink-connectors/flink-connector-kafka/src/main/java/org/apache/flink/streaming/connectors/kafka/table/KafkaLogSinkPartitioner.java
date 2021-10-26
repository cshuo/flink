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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.KeyedRowData;
import org.apache.flink.types.RowKind;

/**
 * Consistent with stream partitioner of file store.
 *
 * <p>See RowWriter.logRow().
 *
 * <p>See BucketStreamPartitioner.
 */
public class KafkaLogSinkPartitioner extends FlinkKafkaPartitioner<RowData> {

    @Override
    public int partition(
            RowData record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        int hash;
        if (record instanceof KeyedRowData) {
            hash = ((KeyedRowData) record).getKey().hashCode();
        } else {
            RowKind rowKind = record.getRowKind();
            record.setRowKind(RowKind.INSERT);
            hash = record.hashCode();
            record.setRowKind(rowKind);
        }
        return partitions[hash % partitions.length];
    }
}
