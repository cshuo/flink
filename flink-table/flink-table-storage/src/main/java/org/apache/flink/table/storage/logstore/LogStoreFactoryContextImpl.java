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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;

import java.util.Map;

/** */
public class LogStoreFactoryContextImpl implements LogStoreFactory.Context {

    private final ObjectIdentifier objectIdentifier;
    private final ResolvedSchema schema;
    private final Map<String, String> options;
    private final ReadableConfig configuration;
    private final ClassLoader classLoader;
    private final boolean isTemporary;
    private final int numBucket;

    public LogStoreFactoryContextImpl(
            ObjectIdentifier objectIdentifier,
            ResolvedSchema schema,
            Map<String, String> options,
            ReadableConfig configuration,
            ClassLoader classLoader,
            boolean isTemporary,
            int numBucket) {
        this.objectIdentifier = objectIdentifier;
        this.schema = schema;
        this.options = options;
        this.configuration = configuration;
        this.classLoader = classLoader;
        this.isTemporary = isTemporary;
        this.numBucket = numBucket;
    }

    @Override
    public ObjectIdentifier getObjectIdentifier() {
        return objectIdentifier;
    }

    @Override
    public ResolvedSchema getSchema() {
        return schema;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public ReadableConfig getConfiguration() {
        return configuration;
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
    public int getNumBucket() {
        return numBucket;
    }
}
