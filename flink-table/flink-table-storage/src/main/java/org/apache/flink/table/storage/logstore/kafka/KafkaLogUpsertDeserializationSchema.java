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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/** */
public class KafkaLogUpsertDeserializationSchema implements KafkaDeserializationSchema<RowData> {

    private final int[] keyIndexes;
    private final RowData.FieldGetter[] keyFieldGetters;
    private final DeserializationSchema<RowData> keyDeserializer;
    private final DeserializationSchema<RowData> valueDeserializer;
    private final TypeInformation<RowData> producedType;

    public KafkaLogUpsertDeserializationSchema(
            int[] keyIndexes,
            RowData.FieldGetter[] keyFieldGetters,
            DeserializationSchema<RowData> keyDeserializer,
            DeserializationSchema<RowData> valueDeserializer,
            TypeInformation<RowData> producedType) {
        this.keyIndexes = keyIndexes;
        this.keyFieldGetters = keyFieldGetters;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.producedType = producedType;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        if (keyDeserializer != null) {
            keyDeserializer.open(context);
        }
        valueDeserializer.open(context);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public RowData deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        if (record.value() == null) {
            GenericRowData key = (GenericRowData) keyDeserializer.deserialize(record.key());
            GenericRowData value = new GenericRowData(RowKind.DELETE, producedType.getArity());
            for (int i = 0; i < keyIndexes.length; i++) {
                value.setField(keyIndexes[i], keyFieldGetters[i].getFieldOrNull(key));
            }
            return value;
        } else {
            return valueDeserializer.deserialize(record.value());
        }
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }
}
