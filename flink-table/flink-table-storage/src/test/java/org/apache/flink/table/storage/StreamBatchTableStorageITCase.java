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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.connectors.kafka.table.KafkaTableTestBase;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;

/** */
public class StreamBatchTableStorageITCase extends KafkaTableTestBase {

    private TableEnvironment batchEnv;

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

        batchEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        batchEnv.getConfig().addConfiguration(configuration);
        batchEnv.getConfig().getConfiguration().set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);

        String catalogName = "my_catalog";
        GenericInMemoryCatalog catalog = new GenericInMemoryCatalog(catalogName);
        batchEnv.registerCatalog(catalogName, catalog);
        batchEnv.useCatalog(catalogName);
        tEnv.registerCatalog(catalogName, catalog);
        tEnv.useCatalog(catalogName);

        tEnv.executeSql(
                "CREATE TABLE src (t TIMESTAMP_LTZ(3), a INT, b INT) WITH (\n"
                        + "'connector' = 'datagen',\n"
                        + "'rows-per-second' = '4000',\n"
                        + "'fields.a.kind' = 'sequence',\n"
                        + "'fields.a.start' = '10000',\n"
                        + "'fields.a.end' = '70000',\n"
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

        TableResult result = tEnv.executeSql("INSERT INTO t SELECT a, b FROM src");

        while (true) {
            try {
                result.await(1, TimeUnit.SECONDS);
                break;
            } catch (TimeoutException ignored) {
            }
            checkSequence(-1);
        }
        checkSequence(60000);
    }

    private void checkSequence(int count) throws Exception {
        CloseableIterator<Row> iterator = batchEnv.executeSql("SELECT * FROM t").collect();
        int expect = 10000;
        boolean fail = false;
        List<Integer> keys = new ArrayList<>();
        int failKey = -1;
        while (iterator.hasNext()) {
            int a = (int) iterator.next().getField(0);
            keys.add(a);
            if (expect != a) {
                fail = true;
                failKey = a;
                while (iterator.hasNext()) {
                    keys.add((Integer) iterator.next().getField(0));
                }
                break;
            }
            expect++;
        }
        iterator.close();

        if (count != -1) {
            Assert.assertEquals(expect - 10000, count);
        }

        if (fail) {
            Assert.fail(String.format("fail key is %s, unexpected result is %s", failKey, keys));
        }
    }

    @Test
    public void testRandomPk() throws Exception {
        tEnv.executeSql("CREATE TABLE t (a INT, b BIGINT, PRIMARY KEY(b) NOT ENFORCED)");

        TableResult result = tEnv.executeSql("INSERT INTO t SELECT a, b FROM src");

        while (true) {
            try {
                result.await(1, TimeUnit.SECONDS);
                break;
            } catch (TimeoutException ignored) {
            }
            checkRandom();
        }
        checkRandom();
    }

    private void checkRandom() throws Exception {
        CloseableIterator<Row> iterator = batchEnv.executeSql("SELECT * FROM t").collect();
        boolean fail = false;
        List<Long> keys = new ArrayList<>();
        long previous = Long.MIN_VALUE;
        while (iterator.hasNext()) {
            long b = (long) iterator.next().getField(1);
            keys.add(b);
            if (b < previous) {
                fail = true;
                previous = b;
                while (iterator.hasNext()) {
                    keys.add((long) iterator.next().getField(1));
                }
                break;
            }
            previous = b;
        }
        iterator.close();

        if (fail) {
            Assert.fail(String.format("fail key is %s, unexpected result is %s", previous, keys));
        }

        Assert.assertTrue(keys.size() > 0);
    }

    @Test
    public void testSequenceBoundedWithPk() throws Exception {
        tEnv.executeSql("CREATE TABLE t (a INT, b BIGINT, PRIMARY KEY(a) NOT ENFORCED)");

        TableResult result =
                tEnv.executeSql("INSERT INTO t SELECT mod(a, 1000) + 10000, b FROM src");

        while (true) {
            try {
                result.await(1, TimeUnit.SECONDS);
                break;
            } catch (TimeoutException ignored) {
            }
            checkSequence(-1);
        }
        checkSequence(1000);
    }
}
