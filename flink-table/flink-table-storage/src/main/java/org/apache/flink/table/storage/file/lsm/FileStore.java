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

package org.apache.flink.table.storage.file.lsm;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.storage.file.lsm.sst.SstFileMeta;

import java.io.Serializable;
import java.util.List;

/** */
public interface FileStore {

    void put(RowData key, RowData value) throws StoreException;

    void delete(RowData key, RowData value) throws StoreException;

    void snapshot(List<SstFileMeta> addFiles, List<SstFileMeta> deleteFiles) throws StoreException;

    KeyValueIterator<RowData, RowData> iterator() throws StoreException;

    /** */
    interface Factory extends Serializable {
        FileStore create(String partition, int bucket, List<SstFileMeta> sstFiles);
    }
}
