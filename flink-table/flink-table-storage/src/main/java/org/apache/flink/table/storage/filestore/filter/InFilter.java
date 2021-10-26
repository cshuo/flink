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

package org.apache.flink.table.storage.filestore.filter;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/** */
public class InFilter<T> implements Filter<T> {

    private final List<T> values;
    private final Comparator<T> comparator;

    public InFilter(List<T> values, Comparator<T> comparator) {
        this.values = values;
        this.comparator = comparator;
    }

    @Override
    public boolean test(T min, T max) {
        for (T v : values) {
            if (comparator.compare(v, min) >= 0 || comparator.compare(v, max) <= 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean test(T t) {
        for (T v : values) {
            if (comparator.compare(v, t) == 0) {
                return true;
            }
        }
        return false;
    }

    public static InFilter<String> equalPartition(String partition) {
        return inPartition(Collections.singletonList(partition));
    }

    public static InFilter<String> inPartition(List<String> partitions) {
        return new InFilter<>(partitions, String::compareTo);
    }
}
