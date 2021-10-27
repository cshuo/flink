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

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.debezium.DebeziumJsonDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.storage.logstore.LogStoreFactory;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.IsolationLevel;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;

import static org.apache.flink.table.storage.logstore.kafka.KafkaLogStoreFactory.projectRow;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;

/** */
public class KafkaLogSourceProvider implements LogStoreFactory.LogSourceProvider {

    private final String topic;

    private final Properties properties;

    private final RowType physicalType;

    private final int[] keyIndexes;

    public KafkaLogSourceProvider(
            String topic, Properties properties, RowType physicalType, int[] keyIndexes) {
        this.topic = topic;
        this.properties = properties;
        this.physicalType = physicalType;
        this.keyIndexes = keyIndexes;
    }

    @Override
    public Source<RowData, ?, ?> createSource(
            LogStoreFactory.LogScanStartupMode startupMode,
            @Nullable Map<Integer, Long> bucketOffsets) {
        if (keyIndexes.length == 0) {
            // enable kafka transaction for non-primary-key table
            properties.setProperty(
                    ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.name().toLowerCase());
        }

        return KafkaSource.<RowData>builder()
                .setTopics(topic)
                .setStartingOffsets(toOffsetsInitializer(startupMode, bucketOffsets))
                .setProperties(properties)
                .setDeserializer(createDeserializer())
                .build();
    }

    private KafkaRecordDeserializationSchema<RowData> createDeserializer() {
        InternalTypeInfo<RowData> producedTypeInfo = InternalTypeInfo.of(physicalType);
        if (keyIndexes.length > 0) {
            RowType keyType = projectRow(physicalType, keyIndexes);
            RowData.FieldGetter[] keyFieldGetters =
                    IntStream.range(0, keyIndexes.length)
                            .mapToObj(i -> RowData.createFieldGetter(keyType.getTypeAt(i), i))
                            .toArray(RowData.FieldGetter[]::new);
            return KafkaRecordDeserializationSchema.of(
                    new KafkaLogUpsertDeserializationSchema(
                            keyIndexes,
                            keyFieldGetters,
                            avroDeserializer(keyType),
                            avroDeserializer(physicalType),
                            producedTypeInfo));
        } else {
            return KafkaRecordDeserializationSchema.valueOnly(
                    new DebeziumJsonDeserializationSchema(
                            TypeConversions.fromLogicalToDataType(physicalType),
                            new ArrayList<>(),
                            producedTypeInfo,
                            false,
                            true,
                            TimestampFormat.SQL));
        }
    }

    private AvroRowDataDeserializationSchema avroDeserializer(RowType type) {
        return new AvroRowDataDeserializationSchema(type, InternalTypeInfo.of(type));
    }

    private OffsetsInitializer toOffsetsInitializer(
            LogStoreFactory.LogScanStartupMode startupMode,
            @Nullable Map<Integer, Long> bucketOffsets) {
        switch (startupMode) {
            case INITIAL:
                if (bucketOffsets == null) {
                    return OffsetsInitializer.earliest();
                } else {
                    return OffsetsInitializer.offsets(toKafkaOffsets(bucketOffsets));
                }
            case LATEST_OFFSET:
                return OffsetsInitializer.latest();
            default:
                throw new UnsupportedOperationException("Unsupported mode: " + startupMode);
        }
    }

    private Map<TopicPartition, Long> toKafkaOffsets(Map<Integer, Long> bucketOffsets) {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        bucketOffsets.forEach(
                (bucket, offset) -> offsets.put(new TopicPartition(topic, bucket), offset));
        return offsets;
    }
}
