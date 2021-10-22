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

package org.apache.flink.table.factories.listener;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.Map;

/**
 * A notification of the operation of a table. Provides catalog and session information describing
 * the dynamic table to be accessed.
 */
@Internal
public interface TableNotification {

    /** Returns the identifier of the table in the {@link Catalog}. */
    ObjectIdentifier getObjectIdentifier();

    /** Returns the table information. */
    CatalogTable getCatalogTable();

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

    /** Creates a copy of this instance with new options. */
    TableNotification copy(Map<String, String> newOptions);
}
