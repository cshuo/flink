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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.Map;

/** The implementation of {@link TableNotification}. */
public class TableNotificationImpl implements TableNotification {

    private final ObjectIdentifier identifier;

    private final CatalogTable table;

    private final ReadableConfig config;

    private final ClassLoader classLoader;

    private final boolean isTemporary;

    public TableNotificationImpl(
            ObjectIdentifier identifier,
            CatalogTable table,
            ReadableConfig config,
            ClassLoader classLoader,
            boolean isTemporary) {
        this.identifier = identifier;
        this.table = table;
        this.config = config;
        this.classLoader = classLoader;
        this.isTemporary = isTemporary;
    }

    @Override
    public ObjectIdentifier getObjectIdentifier() {
        return identifier;
    }

    @Override
    public CatalogTable getCatalogTable() {
        return table;
    }

    @Override
    public ReadableConfig getConfiguration() {
        return config;
    }

    @Override
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    @Override
    public boolean isTemporary() {
        return isTemporary;
    }

    @Override
    public TableNotification copy(Map<String, String> newOptions) {
        CatalogTable newTable = table.copy(newOptions);
        return new TableNotificationImpl(identifier, newTable, config, classLoader, isTemporary);
    }
}
