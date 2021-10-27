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

package org.apache.flink.table.storage.logstore.kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.runtime.sink.BucketRowData;

import org.apache.kafka.clients.producer.ProducerRecord;

/** */
public class KafkaLogSerializationSchema implements KafkaRecordSerializationSchema<RowData> {

    private final String topic;
    private final RowData.FieldGetter[] keyFieldGetters;
    private final SerializationSchema<RowData> keySerializer;
    private final SerializationSchema<RowData> valueSerializer;

    public KafkaLogSerializationSchema(
            String topic,
            RowData.FieldGetter[] keyFieldGetters,
            SerializationSchema<RowData> keySerializer,
            SerializationSchema<RowData> valueSerializer) {
        this.topic = topic;
        this.keyFieldGetters = keyFieldGetters;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void open(
            SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext)
            throws Exception {
        if (keySerializer != null) {
            keySerializer.open(context);
        }
        valueSerializer.open(context);
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            RowData element, KafkaSinkContext context, Long timestamp) {
        byte[] key = null;
        if (keyFieldGetters.length > 0) {
            key = keySerializer.serialize(project(element, keyFieldGetters));
        }
        return new ProducerRecord<>(
                topic,
                ((BucketRowData) element).getBucket(),
                key,
                valueSerializer.serialize(element));
    }

    private static RowData project(RowData row, RowData.FieldGetter[] fieldGetters) {
        GenericRowData projected = new GenericRowData(fieldGetters.length);
        for (int fieldPos = 0; fieldPos < fieldGetters.length; fieldPos++) {
            projected.setField(fieldPos, fieldGetters[fieldPos].getFieldOrNull(row));
        }
        return projected;
    }
}
