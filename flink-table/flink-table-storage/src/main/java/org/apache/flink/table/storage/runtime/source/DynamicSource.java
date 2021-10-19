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

package org.apache.flink.table.storage.runtime.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.storage.file.DynamicTable;
import org.apache.flink.table.storage.file.lsm.FileStore;
import org.apache.flink.table.storage.runtime.RowReader;

import java.util.List;

/** */
public class DynamicSource implements Source<RowData, DynamicSplit, Void> {

    private final DynamicTable table;
    private final FileStore.Factory factory;
    private final RowReader rowReader;
    private final RowDataSerializer keySerializer;
    private final List<String> partitions;

    public DynamicSource(
            DynamicTable table,
            FileStore.Factory factory,
            RowReader rowReader,
            RowDataSerializer keySerializer,
            List<String> partitions) {
        this.table = table;
        this.factory = factory;
        this.rowReader = rowReader;
        this.keySerializer = keySerializer;
        this.partitions = partitions;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<RowData, DynamicSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        return new DynamicReader(readerContext, factory, rowReader);
    }

    @Override
    public SplitEnumerator<DynamicSplit, Void> createEnumerator(
            SplitEnumeratorContext<DynamicSplit> enumContext) throws Exception {
        return new DynamicSplitEnumerator(enumContext, table, partitions);
    }

    @Override
    public SplitEnumerator<DynamicSplit, Void> restoreEnumerator(
            SplitEnumeratorContext<DynamicSplit> enumContext, Void checkpoint) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SimpleVersionedSerializer<DynamicSplit> getSplitSerializer() {
        return new DynamicSplitSerializer(keySerializer);
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return null;
    }
}
