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
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.DefaultDynamicTableFactory;
import org.apache.flink.table.factories.DefaultLogTableFactory;

/**
 * An object that can receive a notification when a table is dropped. Notification occurs before
 * {@link Catalog#dropTable} is called.
 *
 * <p>Only {@link DefaultDynamicTableFactory} and {@link DefaultLogTableFactory} support
 * notification.
 */
@Internal
public interface DropTableListener {

    /** Notifies the listener that a table drop occurred. */
    void onTableDrop(TableNotification context);
}
