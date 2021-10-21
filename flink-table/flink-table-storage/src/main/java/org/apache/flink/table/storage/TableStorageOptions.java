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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.MemorySize;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT_SUFFIX;

/** */
public class TableStorageOptions {

    public static final String TABLE_STORAGE_PREFIX = "table-storage.";

    public static final ConfigOption<Boolean> CHANGE_TRACKING =
            ConfigOptions.key("change-tracking")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("");

    public static final ConfigOption<String> FILE_ROOT_PATH =
            ConfigOptions.key("file.root-path").stringType().noDefaultValue().withDescription("");

    public static final ConfigOption<String> FILE_FORMAT =
            ConfigOptions.key("file" + FORMAT_SUFFIX)
                    .stringType()
                    .defaultValue("parquet")
                    .withDescription("");

    public static final ConfigOption<String> FILE_META_FORMAT =
            ConfigOptions.key("file.meta" + FORMAT_SUFFIX)
                    .stringType()
                    .defaultValue("avro")
                    .withDescription("");

    public static final ConfigOption<MemorySize> FILE_META_TARGET_FILE_SIZE =
            ConfigOptions.key("file.meta.target-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(6))
                    .withDescription("");

    public static final ConfigOption<MemorySize> FILE_TARGET_FILE_SIZE =
            ConfigOptions.key("file.target-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(128))
                    .withDescription("");

    public static final ConfigOption<MemorySize> FILE_MIN_FILE_SIZE =
            ConfigOptions.key("file.min-file-size")
                    .memoryType()
                    .defaultValue(MemorySize.ofMebiBytes(1))
                    .withDescription("");

    public static final ConfigOption<Integer> SNAPSHOTS_NUM_RETAINED =
            ConfigOptions.key("snapshots.num-retained")
                    .intType()
                    .defaultValue(2)
                    .withDescription("The maximum number of completed snapshots to retain.");

    public static final ConfigOption<Integer> FILE_NUM_LEVELS =
            ConfigOptions.key("file.num-levels").intType().defaultValue(4).withDescription("");

    public static final ConfigOption<Integer> FILE_LEVEL0_NUM_FILES =
            ConfigOptions.key("file.level0.num-files")
                    .intType()
                    .defaultValue(4)
                    .withDescription("");

    public static final ConfigOption<Integer> FILE_COMPACTION_MAX_SIZE_AMPLIFICATION_PERCENT =
            ConfigOptions.key("file.compaction.max-size-amplification-percent")
                    .intType()
                    .defaultValue(200)
                    .withDescription("");

    public static final ConfigOption<Integer> FILE_COMPACTION_SIZE_RATIO =
            ConfigOptions.key("file.compaction.size-ratio")
                    .intType()
                    .defaultValue(1)
                    .withDescription("");

    public static final ConfigOption<Boolean> FILE_BINARY_MEM_TABLE_ENABLED =
            ConfigOptions.key("mem-table.binary-enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("");
    public static final ConfigOption<MemorySize> TABLE_EXEC_RESOURCE_EXTERNAL_BUFFER_MEMORY =
            key("mem-table.memory")
                    .memoryType()
                    .defaultValue(MemorySize.parse("32 mb"))
                    .withDescription("");
}
