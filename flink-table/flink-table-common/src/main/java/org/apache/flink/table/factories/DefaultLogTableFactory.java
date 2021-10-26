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

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collection;
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

    @Override
    default String factoryIdentifier() {
        return "_default_log";
    }

    /**
     * Notifies the listener that a table creation occurred.
     *
     * @return new options of this table.
     */
    Map<String, String> onTableCreation(Context context, int numBucket);

    /** Notifies the listener that a table drop occurred. */
    void onTableDrop(Context context);

    // -------------------------- Hybrid consuming -----------------------------

    /**
     * Notifies the listener that a table consuming occurred.
     *
     * @return new options of this table for consuming.
     */
    Map<String, String> onTableConsuming(
            Context context, @Nullable Map<Integer, Long> bucketOffsets);

    /** Gets factory for creating a {@link OffsetsRetriever}. */
    OffsetsRetrieverFactory createOffsetsRetrieverFactory(Context context);

    /** Factory to create {@link OffsetsRetriever}. */
    interface OffsetsRetrieverFactory extends Serializable {

        OffsetsRetriever create();
    }

    /**
     * An interface that provides necessary information to get the initial offsets of the Log
     * buckets.
     */
    interface OffsetsRetriever {

        /** Get the end offsets for the given buckets. */
        Map<Integer, Long> endOffsets(Collection<Integer> buckets);
    }
}
