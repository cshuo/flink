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

package org.apache.flink.table.storage.logstore;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.DescribedEnum;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.description.InlineElement;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DefaultDynamicTableFactory;
import org.apache.flink.table.factories.Factory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;

import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Base interface for configuring a default log table connector. The log table is used by {@link
 * DefaultDynamicTableFactory}.
 *
 * <p>Log tables are for processing only unbounded data. Support streaming reading and streaming
 * writing.
 */
public interface LogStoreFactory extends Factory {

    /** Notifies the listener that a table creation occurred. */
    void onTableCreation(Context context);

    /** Notifies the listener that a table drop occurred. */
    void onTableDrop(Context context);

    LogSourceProvider getSourceProvider(Context context);

    LogSinkProvider getSinkProvider(Context context);

    /** */
    interface Context {

        /** Returns the identifier of the table in the {@link Catalog}. */
        ObjectIdentifier getObjectIdentifier();

        ResolvedSchema getSchema();

        Map<String, String> getOptions();

        /** Gives read-only access to the configuration of the current session. */
        ReadableConfig getConfiguration();

        /**
         * Returns the class loader of the current session.
         *
         * <p>The class loader is in particular useful for discovering further (nested) factories.
         */
        ClassLoader getClassLoader();

        /** Whether the table is temporary. */
        boolean isTemporary();

        int getNumBucket();
    }

    /** */
    interface LogSourceProvider extends Serializable {

        /** Creates a {@link Source} instance. */
        Source<RowData, ?, ?> createSource(
                LogScanStartupMode startupMode, @Nullable Map<Integer, Long> bucketOffsets);
    }

    /** */
    interface LogSinkProvider extends Serializable {

        /** Creates a {@link Sink} instance. */
        Sink<RowData, ?, ?, ?> createSink();

        OffsetsRetriever createOffsetsRetriever();
    }

    /** Specifies the startup mode for log consumer. */
    enum LogScanStartupMode implements DescribedEnum {
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

    /**
     * An interface that provides necessary information to get the initial offsets of the Log
     * buckets.
     */
    interface OffsetsRetriever extends AutoCloseable {

        /** Get the end offsets for the given buckets. */
        Map<Integer, Long> endOffsets(int[] buckets);
    }
}
