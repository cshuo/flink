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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.ScanStartupMode;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.factories.DefaultLogTableFactory;
import org.apache.flink.util.TimeUtils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.DELIVERY_GUARANTEE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.KEY_FORMAT;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_STARTUP_SPECIFIC_OFFSETS;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SINK_PARTITIONER;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TRANSACTIONAL_ID_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FORMAT;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.PROPERTIES_PREFIX;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;

/** The Kafka {@link DefaultLogTableFactory} implementation. */
public class KafkaDefaultLogTableFactory implements DefaultLogTableFactory {

    public static final ConfigOption<Duration> RETENTION =
            ConfigOptions.key("retention").durationType().noDefaultValue().withDescription("");

    @Override
    public Map<String, String> onTableCreation(Context context, int numBucket) {
        CatalogTable table = context.getCatalogTable();
        Optional<Schema.UnresolvedPrimaryKey> primaryKey =
                table.getUnresolvedSchema().getPrimaryKey();

        // 1. create topic
        Map<String, String> options = table.getOptions();
        String topic = topic(context.getObjectIdentifier());
        Duration retention =
                Optional.ofNullable(options.get(RETENTION.key()))
                        .map(TimeUtils::parseDuration)
                        .orElse(null);
        createTopic(options, topic, numBucket, retention, primaryKey.isPresent());

        // 2. create new table options
        Map<String, String> newOptions = new HashMap<>(options);
        if (primaryKey.isPresent()) {
            setIfAbsent(newOptions, CONNECTOR, UpsertKafkaDynamicTableFactory.IDENTIFIER);
            setIfAbsent(newOptions, KEY_FORMAT, "avro");
            setIfAbsent(newOptions, VALUE_FORMAT, "avro");
        } else {
            setIfAbsent(newOptions, CONNECTOR, KafkaDynamicTableFactory.IDENTIFIER);
            // TODO implement debezium avro
            setIfAbsent(newOptions, FORMAT, "debezium-json");

            // set EXACTLY_ONCE guarantee
            if (!newOptions.containsKey(DELIVERY_GUARANTEE.key())) {
                newOptions.put(DELIVERY_GUARANTEE.key(), DeliveryGuarantee.EXACTLY_ONCE.toString());
                newOptions.put(TRANSACTIONAL_ID_PREFIX.key(), "kafka-sink");
                newOptions.put(
                        PROPERTIES_PREFIX + ISOLATION_LEVEL_CONFIG,
                        IsolationLevel.READ_COMMITTED.name().toLowerCase());
            }
        }

        newOptions.put(SINK_PARTITIONER.key(), KafkaLogSinkPartitioner.class.getName());

        setIfAbsent(newOptions, TOPIC, topic);

        return newOptions;
    }

    @Override
    public void onTableDrop(Context context) {
        Map<String, String> options = context.getCatalogTable().getOptions();
        deleteTopic(options, options.get(TOPIC.key()));
    }

    @Override
    public Map<String, String> onTableScan(
            Context context,
            LogScanStartupMode scanStartupMode,
            @Nullable Map<Integer, Long> bucketOffsets) {
        Map<String, String> newOptions = new HashMap<>(context.getCatalogTable().getOptions());
        switch (scanStartupMode) {
            case INITIAL:
                if (bucketOffsets == null) {
                    newOptions.put(
                            SCAN_STARTUP_MODE.key(), ScanStartupMode.EARLIEST_OFFSET.toString());
                } else {
                    newOptions.put(
                            SCAN_STARTUP_MODE.key(), ScanStartupMode.SPECIFIC_OFFSETS.toString());
                    newOptions.put(
                            SCAN_STARTUP_SPECIFIC_OFFSETS.key(),
                            toKafkaOffsetsValue(bucketOffsets));
                }
                break;
            case LATEST_OFFSET:
                newOptions.put(SCAN_STARTUP_MODE.key(), ScanStartupMode.LATEST_OFFSET.toString());
                break;
        }
        return newOptions;
    }

    private String toKafkaOffsetsValue(Map<Integer, Long> bucketOffsets) {
        return bucketOffsets.entrySet().stream()
                .map(entry -> "partition:" + entry.getKey() + ",offset:" + entry.getValue())
                .collect(Collectors.joining(";"));
    }

    @Override
    public OffsetsRetrieverFactory createOffsetsRetrieverFactory(Context context) {
        Properties properties =
                KafkaConnectorOptionsUtil.getKafkaProperties(
                        context.getCatalogTable().getOptions());
        String topic = topic(context.getObjectIdentifier());
        return new KafkaBucketOffsetsRetrieverFactory(properties, topic);
    }

    private void setIfAbsent(Map<String, String> options, ConfigOption<?> option, String value) {
        if (!options.containsKey(option.key())) {
            options.put(option.key(), value);
        }
    }

    private static String topic(ObjectIdentifier identifier) {
        return identifier.asSummaryString();
    }

    private static void createTopic(
            Map<String, String> options,
            String topic,
            int numberOfPartitions,
            Duration retention,
            boolean compact) {
        Properties properties = KafkaConnectorOptionsUtil.getKafkaProperties(options);
        try (AdminClient adminClient = AdminClient.create(properties)) {
            Map<String, String> configs = new HashMap<>();
            if (retention != null) {
                configs.put(
                        TopicConfig.RETENTION_BYTES_CONFIG, String.valueOf(retention.toMillis()));
            }

            if (compact) {
                configs.put(
                        TopicConfig.CLEANUP_POLICY_CONFIG,
                        TopicConfig.CLEANUP_POLICY_COMPACT
                                + ","
                                + TopicConfig.CLEANUP_POLICY_DELETE);
            }

            NewTopic topicObj =
                    new NewTopic(topic, Optional.of(numberOfPartitions), Optional.empty())
                            .configs(configs);

            adminClient.createTopics(Collections.singleton(topicObj)).all().get();
        } catch (ExecutionException | InterruptedException e) {
            throw new TableException("Error in createTopic", e);
        }
    }

    private static void deleteTopic(Map<String, String> options, String topic) {
        Properties properties = KafkaConnectorOptionsUtil.getKafkaProperties(options);
        try (AdminClient adminClient = AdminClient.create(properties)) {
            adminClient.deleteTopics(Collections.singleton(topic)).all().get();
        } catch (ExecutionException | InterruptedException e) {
            throw new TableException("Error in deleteTopic", e);
        }
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    private static class KafkaBucketOffsetsRetrieverFactory implements OffsetsRetrieverFactory {

        private final Properties properties;
        private final String topic;

        private KafkaBucketOffsetsRetrieverFactory(Properties properties, String topic) {
            this.properties = properties;
            this.topic = topic;
        }

        @Override
        public OffsetsRetriever create() {
            properties.setProperty(
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    ByteArrayDeserializer.class.getName());
            properties.setProperty(
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    ByteArrayDeserializer.class.getName());
            KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(properties);
            return buckets -> {
                List<TopicPartition> partitions =
                        Arrays.stream(buckets)
                                .mapToObj(bucket -> new TopicPartition(topic, bucket))
                                .collect(Collectors.toList());
                Map<TopicPartition, Long> partitionOffsets = consumer.endOffsets(partitions);
                Map<Integer, Long> offsets = new HashMap<>();
                partitionOffsets.forEach(
                        (partition, offset) -> offsets.put(partition.partition(), offset));
                return offsets;
            };
        }
    }
}
