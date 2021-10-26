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

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** */
public class SortMergeIterator<T> implements DualIterator<T> {

    private final List<DualIterator<T>> iterators;
    private final Comparator<T> comparator;

    private PriorityQueue<Element> priorityQueue;

    private Element previous;

    public SortMergeIterator(List<DualIterator<T>> iterators, Comparator<T> comparator) {
        this.iterators = iterators;
        this.comparator = comparator;
    }

    private boolean advanceFirst() throws IOException {
        this.priorityQueue = new PriorityQueue<>();
        for (DualIterator<T> iter : iterators) {
            Element element = new Element(iter);
            if (element.advanceNext()) {
                priorityQueue.add(element);
            }
        }
        return priorityQueue.peek() != null;
    }

    @Override
    public boolean advanceNext() throws IOException {
        if (priorityQueue == null) {
            return advanceFirst();
        }

        previous = priorityQueue.poll();
        checkNotNull(previous, "Illegal to call after this has returned `false`.");

        if (previous.advanceNext()) {
            priorityQueue.add(previous);
        }
        return priorityQueue.peek() != null;
    }

    @Override
    public T current() {
        Element current = priorityQueue.peek();
        checkNotNull(current, "Illegal to call after advanceNext() has returned `false`.");
        return current.current();
    }

    @Override
    public T previous() {
        return previous.previous();
    }

    @Override
    public void close() throws IOException {
        for (DualIterator<T> iterator : iterators) {
            iterator.close();
        }
    }

    private class Element implements Comparable<Element> {

        private final DualIterator<T> iterator;

        private Element(DualIterator<T> iterator) {
            this.iterator = iterator;
        }

        private boolean advanceNext() throws IOException {
            return iterator.advanceNext();
        }

        private T previous() {
            return iterator.previous();
        }

        private T current() {
            return iterator.current();
        }

        @Override
        public int compareTo(Element o) {
            return comparator.compare(iterator.current(), o.iterator.current());
        }
    }
}
