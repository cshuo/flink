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

import java.util.Map;

/**
 * Base interface for configuring a default dynamic table connector. The default table factory is
 * used when there is no {@link FactoryUtil#CONNECTOR} option.
 */
@Internal
public interface DefaultDynamicTableFactory extends DynamicTableFactory {

    @Override
    default String factoryIdentifier() {
        return "_default";
    }

    /**
     * Notifies the listener that a table creation occurred.
     *
     * @return new options of this table.
     */
    Map<String, String> onTableCreation(Context context);

    /** Notifies the listener that a table drop occurred. */
    void onTableDrop(Context context);

    /**
     * Discovers the unique implementation of {@link DefaultDynamicTableFactory} without identifier.
     */
    static DefaultDynamicTableFactory discoverDefaultFactory(ClassLoader classLoader) {
        return FactoryUtil.discoverUniqueImplTableFactory(
                DefaultDynamicTableFactory.class, classLoader);
    }
}
