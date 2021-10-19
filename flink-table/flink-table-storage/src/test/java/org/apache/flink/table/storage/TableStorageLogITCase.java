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

package org.apache.flink.table.storage;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaTestBaseWithFlink;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.streaming.connectors.kafka.table.KafkaTableTestUtils.collectRows;
import static org.apache.flink.table.utils.TableTestMatchers.deepEqualTo;
import static org.junit.Assert.assertThat;

/** */
public class TableStorageLogITCase extends KafkaTestBaseWithFlink {

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;

    @Before
    public void before() throws IOException {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env);
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        env.setParallelism(1);
        String bootstraps = standardProps.getProperty("bootstrap.servers");
        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setString(
                "table-storage.file.root-path", tempFolder.newFolder().toURI().toString());
        configuration.setString("table-storage.log.properties.bootstrap.servers", bootstraps);
    }

    @Test
    public void testWithPrimaryKey() throws Exception {
        // ---------- Produce an event time stream into Kafka -------------------
        final String createTable =
                "CREATE TABLE kafka (\n"
                        + "  `k_user_id` BIGINT,\n"
                        + "  `name` STRING,\n"
                        + "  `k_event_id` BIGINT,\n"
                        + "  `user_id` INT,\n"
                        + "  `payload` STRING,\n"
                        + "  PRIMARY KEY(k_user_id) NOT ENFORCED\n"
                        + ")";

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO kafka\n"
                        + "VALUES\n"
                        + " (1, 'name 1', 100, 41, 'payload 1'),\n"
                        + " (2, 'name 2', 101, 42, 'payload 2'),\n"
                        + " (3, 'name 3', 102, 43, 'payload 3')";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        final List<Row> result = collectRows(tEnv.sqlQuery("SELECT * FROM kafka"), 3);

        final List<Row> expected =
                Arrays.asList(
                        Row.of(1L, "name 1", 100L, 41, "payload 1"),
                        Row.of(2L, "name 2", 101L, 42, "payload 2"),
                        Row.of(3L, "name 3", 102L, 43, "payload 3"));

        assertThat(result, deepEqualTo(expected, true));
    }

    @Test
    public void testWithoutPrimaryKey() throws Exception {
        // ---------- Produce an event time stream into Kafka -------------------
        final String createTable =
                "CREATE TABLE kafka (\n"
                        + "  `k_user_id` BIGINT,\n"
                        + "  `name` STRING,\n"
                        + "  `k_event_id` BIGINT,\n"
                        + "  `user_id` INT,\n"
                        + "  `payload` STRING\n"
                        + ")";

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO kafka\n"
                        + "VALUES\n"
                        + " (1, 'name 1', 100, 41, 'payload 1'),\n"
                        + " (2, 'name 2', 101, 42, 'payload 2'),\n"
                        + " (3, 'name 3', 102, 43, 'payload 3')";
        tEnv.executeSql(initialValues).await();

        // ---------- Consume stream from Kafka -------------------

        final List<Row> result = collectRows(tEnv.sqlQuery("SELECT * FROM kafka"), 3);

        final List<Row> expected =
                Arrays.asList(
                        Row.of(1L, "name 1", 100L, 41, "payload 1"),
                        Row.of(2L, "name 2", 101L, 42, "payload 2"),
                        Row.of(3L, "name 3", 102L, 43, "payload 3"));

        assertThat(result, deepEqualTo(expected, true));
    }
}
