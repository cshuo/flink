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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;

/** */
@Internal
public interface BuiltInDynamicTableFactory extends DynamicTableFactory {

    @Override
    default String factoryIdentifier() {
        return "_built-in";
    }

    CatalogTable onCreateTable(Context context);

    void onDropTable(Context context);

    static BuiltInDynamicTableFactory discoverFactory(ClassLoader classLoader) {
        return FactoryUtil.discoverSingletonTableFactory(
                BuiltInDynamicTableFactory.class, classLoader);
    }

    static BuiltInLogTableFactory discoverLogFactory(ClassLoader classLoader) {
        return FactoryUtil.discoverSingletonTableFactory(BuiltInLogTableFactory.class, classLoader);
    }

    /** */
    interface Context {

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

        Context copy(CatalogTable newTable);
    }
}
