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
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.connectors.kafka.table.KafkaTableTestBase;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;

/** */
public class StreamTableStorageITCase extends KafkaTableTestBase {

    @Before
    public void before() {
        URI folder;
        try {
            folder = TEMPORARY_FOLDER.newFolder().toURI();
            System.out.println(folder.getPath());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Configuration configuration = tEnv.getConfig().getConfiguration();
        configuration.setString("table-storage.file.root-path", folder.toString());
        configuration.setString("table-storage.file.target-file-size", "200 k");
        configuration.setString("table-storage.file.min-file-size", "200 k");
        configuration.setString(
                "table-storage.log.properties.bootstrap.servers", getBootstrapServers());
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        // run both stream and batch
        configuration.set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        tEnv.executeSql(
                "CREATE TABLE src (t TIMESTAMP_LTZ(3), a INT, b INT) WITH (\n"
                        + "'connector' = 'datagen',\n"
                        + "'rows-per-second' = '400',\n"
                        + "'fields.a.kind' = 'sequence',\n"
                        + "'fields.a.start' = '1000',\n"
                        + "'fields.a.end' = '7000',\n"
                        + "'number-of-rows' = '100000'"
                        + ")");
    }

    @After
    public void after() {
        super.after();
        tEnv.executeSql("DROP TABLE t");
    }

    @Test
    public void testSequenceWithPk() throws Exception {
        tEnv.executeSql("CREATE TABLE t (a INT, b BIGINT, PRIMARY KEY(a) NOT ENFORCED)");
        tEnv.executeSql("INSERT INTO t SELECT a, b FROM src");
        Thread.sleep(5000);
        tEnv.executeSql("SELECT * FROM t").print();
    }

    @Test
    public void testSequenceWithoutPk() throws Exception {
        tEnv.executeSql("CREATE TABLE t (a INT, b BIGINT)");
        tEnv.executeSql("INSERT INTO t SELECT a, b FROM src");
        Thread.sleep(5000);
        tEnv.executeSql("SELECT * FROM t").print();
    }
}
