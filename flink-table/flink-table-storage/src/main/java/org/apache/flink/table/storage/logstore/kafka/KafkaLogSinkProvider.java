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
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.debezium.DebeziumJsonSerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.logstore.LogStoreFactory;
import org.apache.flink.table.storage.logstore.LogStoreFactory.OffsetsRetriever;
import org.apache.flink.table.types.logical.RowType;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.flink.table.storage.logstore.kafka.KafkaLogStoreFactory.projectRow;

/** */
public class KafkaLogSinkProvider implements LogStoreFactory.LogSinkProvider {

    private final String topic;

    private final Properties properties;

    private final RowType physicalType;

    private final int[] keyIndexes;

    public KafkaLogSinkProvider(
            String topic, Properties properties, RowType physicalType, int[] keyIndexes) {
        this.topic = topic;
        this.properties = properties;
        this.physicalType = physicalType;
        this.keyIndexes = keyIndexes;
    }

    @Override
    public Sink<RowData, ?, ?, ?> createSink() {
        KafkaSinkBuilder<RowData> builder = KafkaSink.builder();
        if (keyIndexes.length == 0) {
            builder.setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE);
            builder.setTransactionalIdPrefix("kafka-sink");
        } else {
            builder.setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE);
        }

        return builder.setBootstrapServers(
                        properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG).toString())
                .setKafkaProducerConfig(properties)
                .setRecordSerializer(createSerializer())
                .build();
    }

    private KafkaLogSerializationSchema createSerializer() {
        RowData.FieldGetter[] keyFieldGetters = new RowData.FieldGetter[keyIndexes.length];
        for (int i = 0; i < keyFieldGetters.length; i++) {
            int keyIndex = keyIndexes[i];
            keyFieldGetters[i] =
                    RowData.createFieldGetter(physicalType.getTypeAt(keyIndex), keyIndex);
        }

        SerializationSchema<RowData> keySerializer;
        SerializationSchema<RowData> valueSerializer;

        if (keyIndexes.length > 0) {
            keySerializer =
                    new AvroRowDataSerializationSchema(projectRow(physicalType, keyIndexes));
            valueSerializer = new AvroRowDataSerializationSchema(physicalType);
        } else {
            keySerializer = null;
            valueSerializer =
                    new DebeziumJsonSerializationSchema(
                            physicalType,
                            TimestampFormat.SQL,
                            JsonFormatOptions.MapNullKeyMode.FAIL,
                            null,
                            false);
        }

        return new KafkaLogSerializationSchema(
                topic, keyFieldGetters, keySerializer, valueSerializer);
    }

    @Override
    public OffsetsRetriever createOffsetsRetriever() {
        Properties properties = new Properties();
        properties.putAll(this.properties);
        properties.setProperty(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(properties);

        return new OffsetsRetriever() {

            @Override
            public Map<Integer, Long> endOffsets(int[] buckets) {
                List<TopicPartition> partitions =
                        Arrays.stream(buckets)
                                .mapToObj(bucket -> new TopicPartition(topic, bucket))
                                .collect(Collectors.toList());
                Map<TopicPartition, Long> partitionOffsets = consumer.endOffsets(partitions);
                Map<Integer, Long> offsets = new HashMap<>();
                partitionOffsets.forEach(
                        (partition, offset) -> offsets.put(partition.partition(), offset));
                return offsets;
            }

            @Override
            public void close() {
                consumer.close();
            }
        };
    }
}
