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
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.InlineElement;

import static org.apache.flink.configuration.description.TextElement.text;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT_SUFFIX;

/** */
public class Options {

    public static final String TABLE_STORAGE_PREFIX = "table-storage.";

    public static final ConfigOption<Integer> BUCKET =
            ConfigOptions.key("bucket")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The bucket number of default dynamic table. "
                                    + "The record is hashed into different buckets. "
                                    + "The larger the bucket, the better the concurrent execution.");

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
                    .defaultValue("orc")
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
                    .defaultValue(5)
                    .withDescription("The maximum number of completed snapshots to retain.");

    public static final ConfigOption<Integer> SNAPSHOTS_NUM_EXPIRE_TRIGGER =
            ConfigOptions.key("snapshots.num-expire-trigger")
                    .intType()
                    .defaultValue(10)
                    .withDescription("The number of completed snapshots to trigger expiration.");

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

    public static final ConfigOption<Boolean> FILE_COMMIT_FORCE_COMPACT =
            ConfigOptions.key("file.commit.force-compact")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("");

    public static final ConfigOption<LogScanStartupMode> LOG_SCAN_STARTUP_MODE =
            ConfigOptions.key("log.scan.startup.mode")
                    .enumType(LogScanStartupMode.class)
                    .defaultValue(LogScanStartupMode.INITIAL)
                    .withDescription("Startup mode for log consumer.");

    /** Specifies the startup mode for log consumer. */
    public enum LogScanStartupMode implements DescribedEnum {
        INITIAL(
                "initial",
                text(
                        "Performs an initial snapshot on the table upon first startup,"
                                + " and continue to read the latest changes.")),

        LATEST_OFFSET(
                "latest-offset",
                text(
                        "Never to perform snapshot on the table upon first startup,"
                                + " just read from the end of the log which means only"
                                + " have the changes since the connector was started."));

        private final String value;
        private final InlineElement description;

        LogScanStartupMode(String value, InlineElement description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String toString() {
            return value;
        }

        @Override
        public InlineElement getDescription() {
            return description;
        }
    }
}
