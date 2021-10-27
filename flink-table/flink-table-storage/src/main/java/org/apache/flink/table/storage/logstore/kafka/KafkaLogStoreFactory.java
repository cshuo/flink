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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.factories.DefaultLogTableFactory;
import org.apache.flink.table.storage.logstore.LogStoreFactory;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.TimeUtils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions.TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptionsUtil.getKafkaProperties;

/** The Kafka {@link DefaultLogTableFactory} implementation. */
public class KafkaLogStoreFactory implements LogStoreFactory {

    public static final String IDENTIFIER = "kafka";

    public static final ConfigOption<Duration> RETENTION =
            ConfigOptions.key("retention").durationType().noDefaultValue().withDescription("");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }

    @Override
    public void onTableCreation(Context context) {
        Duration retention =
                Optional.ofNullable(context.getOptions().get(RETENTION.key()))
                        .map(TimeUtils::parseDuration)
                        .orElse(null);
        Properties properties = getKafkaProperties(context.getOptions());
        try (AdminClient adminClient = AdminClient.create(properties)) {
            Map<String, String> configs = new HashMap<>();
            if (retention != null) {
                configs.put(
                        TopicConfig.RETENTION_BYTES_CONFIG, String.valueOf(retention.toMillis()));
            }

            if (context.getSchema().getPrimaryKeyIndexes().length > 0) {
                configs.put(
                        TopicConfig.CLEANUP_POLICY_CONFIG,
                        TopicConfig.CLEANUP_POLICY_COMPACT
                                + ","
                                + TopicConfig.CLEANUP_POLICY_DELETE);
            }

            NewTopic topicObj =
                    new NewTopic(
                                    topic(context),
                                    Optional.of(context.getNumBucket()),
                                    Optional.empty())
                            .configs(configs);

            adminClient.createTopics(Collections.singleton(topicObj)).all().get();
        } catch (ExecutionException | InterruptedException e) {
            throw new TableException("Error in createTopic", e);
        }
    }

    @Override
    public void onTableDrop(Context context) {
        Map<String, String> options = context.getOptions();
        Properties properties = getKafkaProperties(options);
        try (AdminClient adminClient = AdminClient.create(properties)) {
            adminClient.deleteTopics(Collections.singleton(options.get(TOPIC.key()))).all().get();
        } catch (ExecutionException | InterruptedException e) {
            throw new TableException("Error in deleteTopic", e);
        }
    }

    @Override
    public LogSourceProvider getSourceProvider(Context context) {
        return new KafkaLogSourceProvider(
                topic(context),
                getKafkaProperties(context.getOptions()),
                physicalType(context.getSchema()),
                getPrimaryKeyPhysicalIndexes(context.getSchema()));
    }

    @Override
    public LogSinkProvider getSinkProvider(Context context) {
        return new KafkaLogSinkProvider(
                topic(context),
                getKafkaProperties(context.getOptions()),
                physicalType(context.getSchema()),
                getPrimaryKeyPhysicalIndexes(context.getSchema()));
    }

    private static String topic(Context context) {
        return context.getObjectIdentifier().asSummaryString();
    }

    private static RowType physicalType(ResolvedSchema schema) {
        return (RowType) schema.toPhysicalRowDataType().getLogicalType();
    }

    private static int[] getPrimaryKeyPhysicalIndexes(ResolvedSchema schema) {
        Optional<UniqueConstraint> primaryKey = schema.getPrimaryKey();
        if (!primaryKey.isPresent()) {
            return new int[0];
        }

        List<String> fieldNames = physicalType(schema).getFieldNames();
        return primaryKey.get().getColumns().stream().mapToInt(fieldNames::indexOf).toArray();
    }

    public static RowType projectRow(RowType rowType, int[] indices) {
        return (RowType)
                DataTypeUtils.projectRow(TypeConversions.fromLogicalToDataType(rowType), indices)
                        .getLogicalType();
    }
}
