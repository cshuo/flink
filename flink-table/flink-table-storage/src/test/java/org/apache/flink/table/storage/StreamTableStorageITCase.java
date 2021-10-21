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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;

/** */
public class StreamTableStorageITCase extends StreamingTestBase {

    @Before
    public void before() {
        super.before();
        super.before();
        URI folder;
        try {
            folder = TEMPORARY_FOLDER.newFolder().toURI();
            System.out.println(folder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        tEnv().getConfig()
                .getConfiguration()
                .setString("table-storage.file.root-path", folder.toString());
        tEnv().getConfig().getConfiguration().setString("table-storage.change-tracking", "false");
        env().enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env().setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
        tEnv().executeSql(
                        "CREATE TABLE src (t TIMESTAMP_LTZ(3), a INT, b INT) WITH (\n"
                                + "'connector' = 'datagen',\n"
                                + "'rows-per-second' = '10',\n"
                                + "'fields.a.kind' = 'sequence',\n"
                                + "'fields.a.start' = '10000',\n"
                                + "'fields.a.end' = '30000',\n"
                                + "'number-of-rows' = '100000'"
                                + ")");
    }

    @Test
    public void write() throws ExecutionException, InterruptedException {
        // run both stream and batch
        tEnv().getConfig()
                .getConfiguration()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);

        String ddl = "CREATE TABLE t (a INT, b BIGINT, PRIMARY KEY(a) NOT ENFORCED)";
        tEnv().executeSql(ddl);
        TableResult result = tEnv().executeSql("INSERT INTO t SELECT a, b FROM src");

        TableEnvironment batchEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        batchEnv.getConfig().addConfiguration(tEnv().getConfig().getConfiguration());
        batchEnv.getConfig().getConfiguration().set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        batchEnv.executeSql(ddl);

        while (true) {
            try {
                result.await(10, TimeUnit.SECONDS);
                break;
            } catch (TimeoutException ignored) {
            }

            System.out.println("start new query!");
            batchEnv.executeSql("SELECT * FROM t").print();
        }
    }
}
