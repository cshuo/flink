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

package org.apache.flink.table.storage.filestore.manifest;

import java.util.Objects;

/** */
public class FileIdentifier {

    private final String partition;

    private final int bucket;

    private final String name;

    public FileIdentifier(String partition, int bucket, String name) {
        this.partition = partition;
        this.bucket = bucket;
        this.name = name;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder().append("FileIdentifier{");
        if (partition != null) {
            builder.append("partition='").append(partition).append('\'').append(", ");
        }
        return builder.append("bucket=")
                .append(bucket)
                .append(", name='")
                .append(name)
                .append('\'')
                .append('}')
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileIdentifier identifier = (FileIdentifier) o;
        return bucket == identifier.bucket
                && Objects.equals(partition, identifier.partition)
                && Objects.equals(name, identifier.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, bucket, name);
    }
}
