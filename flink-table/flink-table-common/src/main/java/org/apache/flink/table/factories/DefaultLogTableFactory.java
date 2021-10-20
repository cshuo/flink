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

package org.apache.flink.table.factories;

import org.apache.flink.annotation.Internal;

import java.util.HashMap;
import java.util.Map;

/**
 * Base interface for configuring a default log table connector. The log table is used by {@link
 * DefaultDynamicTableFactory}.
 *
 * <p>Log tables are for processing only unbounded data. Support streaming reading and streaming
 * writing.
 */
@Internal
public interface DefaultLogTableFactory extends DynamicTableFactory {

    String LOG_PREFIX = "log.";

    @Override
    default String factoryIdentifier() {
        return "_default_log";
    }

    /** @return log options from table options. */
    static Map<String, String> logOptions(Map<String, String> tableOptions) {
        Map<String, String> options = new HashMap<>();
        tableOptions.forEach(
                (k, v) -> {
                    if (k.startsWith(LOG_PREFIX)) {
                        options.put(k.substring(LOG_PREFIX.length()), v);
                    }
                });
        return options;
    }
}
