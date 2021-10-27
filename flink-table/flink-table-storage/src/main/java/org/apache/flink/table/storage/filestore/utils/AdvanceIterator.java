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

package org.apache.flink.table.storage.filestore.utils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * An internal iterator interface which presents a more restrictive API than {@link Iterator}.
 *
 * <p>One major departure from the Java iterator API is the fusing of the `hasNext()` and `next()`
 * calls: Java's iterator allows users to call `hasNext()` without immediately advancing the
 * iterator to consume the next row, whereas {@link AdvanceIterator} combines these calls into a
 * single {@link #advanceNext()} method.
 */
public interface AdvanceIterator<T> extends Closeable {

    /**
     * Advance this iterator by a single element. Returns false if this iterator has no more
     * elements and true otherwise. If this returns true, then the new element can be retrieved by
     * calling {@link #current()}.
     */
    boolean advanceNext() throws IOException;

    /**
     * Retrieve the element from this iterator. This method is idempotent. It is illegal to call
     * this method after {@link #advanceNext()} has returned false.
     */
    T current();
}
