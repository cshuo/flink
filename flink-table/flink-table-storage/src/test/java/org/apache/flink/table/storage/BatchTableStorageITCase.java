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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.CollectionUtil.iteratorToList;

/** */
public class BatchTableStorageITCase extends BatchTestBase {

    @Before
    public void before() {
        super.before();
        URI folder;
        try {
            folder = TEMPORARY_FOLDER.newFolder().toURI();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        conf().getConfiguration().setString("table-storage.mode", "batch");
        conf().getConfiguration().setString("table-storage.change-tracking", "false");
        conf().getConfiguration().setString("table-storage.file.root-path", folder.toString());
        tEnv().executeSql(
                        "CREATE TABLE partitioned_t "
                                + "(dt STRING, a INT, b BIGINT, PRIMARY KEY(a) NOT ENFORCED) PARTITIONED BY (dt)");
        registerCollection(
                "Table3",
                TestData.data3(),
                (TypeInformation) TestData.type3(),
                "a, b, c",
                TestData.nullablesOfData3());
    }

    @Test
    public void testPrimaryKey() throws ExecutionException, InterruptedException {
        tEnv().executeSql("CREATE TABLE pk_t (a INT, b BIGINT, PRIMARY KEY(b) NOT ENFORCED)");
        tEnv().executeSql("INSERT INTO pk_t SELECT a, b FROM Table3").await();

        List<Row> result = iteratorToList(tEnv().executeSql("SELECT * FROM pk_t").collect());
        result.sort((o1, o2) -> ((Comparable) o1.getField(0)).compareTo(o2.getField(0)));
        Assert.assertEquals(
                Arrays.asList(
                        Row.of(1, 1L),
                        Row.of(3, 2L),
                        Row.of(6, 3L),
                        Row.of(10, 4L),
                        Row.of(15, 5L),
                        Row.of(21, 6L)),
                result);
    }

    @Test
    public void testWithoutPk() throws ExecutionException, InterruptedException {
        tEnv().executeSql("CREATE TABLE t (a INT, b BIGINT)");
        tEnv().executeSql("INSERT INTO t SELECT a, b FROM Table3 WHERE b < 5").await();

        List<Row> result = iteratorToList(tEnv().executeSql("SELECT * FROM t").collect());
        result.sort((o1, o2) -> ((Comparable) o1.getField(0)).compareTo(o2.getField(0)));
        Assert.assertEquals(
                Arrays.asList(
                        Row.of(1, 1L),
                        Row.of(2, 2L),
                        Row.of(3, 2L),
                        Row.of(4, 3L),
                        Row.of(5, 3L),
                        Row.of(6, 3L),
                        Row.of(7, 4L),
                        Row.of(8, 4L),
                        Row.of(9, 4L),
                        Row.of(10, 4L)),
                result);
    }
}
