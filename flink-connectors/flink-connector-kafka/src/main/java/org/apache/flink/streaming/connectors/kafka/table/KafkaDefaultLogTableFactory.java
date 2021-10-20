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
import org.apache.flink.table.factories.listener.CreateTableListener;
import org.apache.flink.table.factories.listener.DropTableListener;
import org.apache.flink.table.factories.listener.TableNotification;
import org.apache.flink.util.TimeUtils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.DELIVERY_GUARANTEE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.KEY_FORMAT;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SCAN_STARTUP_MODE;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.SINK_PARTITIONER;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TRANSACTIONAL_ID_PREFIX;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.VALUE_FORMAT;
import static org.apache.flink.table.factories.DefaultDynamicTableFactory.BUCKET;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;

/** The Kafka {@link DefaultLogTableFactory} implementation. */
public class KafkaDefaultLogTableFactory
        implements DefaultLogTableFactory, CreateTableListener, DropTableListener {

    public static final ConfigOption<Duration> RETENTION =
            ConfigOptions.key("retention").durationType().noDefaultValue().withDescription("");

    @Override
    public Map<String, String> onTableCreation(TableNotification context) {
        CatalogTable table = context.getCatalogTable();
        Map<String, String> tableOptions = table.getOptions();
        Optional<Schema.UnresolvedPrimaryKey> primaryKey =
                table.getUnresolvedSchema().getPrimaryKey();

        // 1. create topic
        Map<String, String> logOptions = DefaultLogTableFactory.logOptions(tableOptions);
        String topic = topic(context.getObjectIdentifier());
        Duration retention =
                Optional.ofNullable(logOptions.get(RETENTION.key()))
                        .map(TimeUtils::parseDuration)
                        .orElse(null);
        int bucket =
                Integer.parseInt(
                        tableOptions.getOrDefault(BUCKET.key(), BUCKET.defaultValue().toString()));
        createTopic(logOptions, topic, bucket, retention, primaryKey.isPresent());

        // 2. create new table options
        Map<String, String> newOptions = new HashMap<>(tableOptions);
        if (primaryKey.isPresent()) {
            setIfAbsent(newOptions, CONNECTOR, UpsertKafkaDynamicTableFactory.IDENTIFIER);
            setIfAbsent(newOptions, KEY_FORMAT, "avro");
            setIfAbsent(newOptions, VALUE_FORMAT, "avro");
        } else {
            setIfAbsent(newOptions, CONNECTOR, KafkaDynamicTableFactory.IDENTIFIER);
            // TODO implement debezium avro
            setIfAbsent(newOptions, FORMAT, "debezium-json");

            // set EXACTLY_ONCE guarantee
            setIfAbsent(newOptions, DELIVERY_GUARANTEE, DeliveryGuarantee.EXACTLY_ONCE.toString());
            // only one writer, we can set a unique value
            setIfAbsent(newOptions, TRANSACTIONAL_ID_PREFIX, "kafka-sink");

            // partition for changes
            setIfAbsent(
                    newOptions, SINK_PARTITIONER, KafkaChangeLogSinkPartitioner.class.getName());

            setIfAbsent(newOptions, SCAN_STARTUP_MODE, ScanStartupMode.EARLIEST_OFFSET.toString());

            // TODO write key instead of SINK_PARTITIONER???
        }

        setIfAbsent(newOptions, TOPIC, topic);

        return newOptions;
    }

    @Override
    public void onTableDrop(TableNotification context) {
        Map<String, String> logOptions =
                DefaultLogTableFactory.logOptions(context.getCatalogTable().getOptions());
        deleteTopic(logOptions, topic(context.getObjectIdentifier()));
    }

    private void setIfAbsent(Map<String, String> options, ConfigOption<?> option, String value) {
        String key = LOG_PREFIX + option.key();
        if (!options.containsKey(key)) {
            options.put(key, value);
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
}
