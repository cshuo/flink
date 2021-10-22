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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.SingletonResultIterator;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.formats.avro.utils.FSDataInputStreamWrapper;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/** */
public class AvroGenericRecordInputFormat<T> implements BulkFormat<T, FileSourceSplit> {

    private final GenericRecordConverter<T> converter;

    public AvroGenericRecordInputFormat(GenericRecordConverter<T> converter) {
        this.converter = converter;
    }

    @Override
    public Reader<T> createReader(Configuration config, FileSourceSplit split) throws IOException {
        FileSystem fileSystem = split.path().getFileSystem();
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        SeekableInput in =
                new FSDataInputStreamWrapper(
                        fileSystem.open(split.path()),
                        fileSystem.getFileStatus(split.path()).getLen());
        DataFileReader<GenericRecord> dataFileReader =
                (DataFileReader<GenericRecord>) DataFileReader.openReader(in, datumReader);

        return new Reader<T>() {
            @Nullable
            @Override
            public RecordIterator<T> readBatch() throws IOException {
                if (dataFileReader.hasNext()) {
                    GenericRecord record = dataFileReader.next();
                    SingletonResultIterator<T> iterator = new SingletonResultIterator<>();
                    iterator.set(converter.convert(record), 0, 0);
                    return iterator;
                }

                return null;
            }

            @Override
            public void close() throws IOException {
                dataFileReader.close();
            }
        };
    }

    @Override
    public Reader<T> restoreReader(Configuration config, FileSourceSplit split) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSplittable() {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return converter.getProducedType();
    }

    /** */
    public interface GenericRecordConverter<T> extends Serializable {
        T convert(GenericRecord record);

        TypeInformation<T> getProducedType();
    }
}
