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

package org.apache.flink.table.storage.runtime.sink;

import org.apache.flink.api.connector.sink.Committer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/** */
public class LocalCommitter<LogCommT> implements Committer<DynamicCommittable<LogCommT>> {

    private final Committer<LogCommT> logCommitter;

    public LocalCommitter(Committer<LogCommT> logCommitter) {
        this.logCommitter = logCommitter;
    }

    @Override
    public List<DynamicCommittable<LogCommT>> commit(
            List<DynamicCommittable<LogCommT>> committables)
            throws IOException, InterruptedException {
        List<LogCommT> logCommittables = new ArrayList<>();
        for (DynamicCommittable<LogCommT> committable : committables) {
            logCommittables.addAll(committable.getLogCommittables());
        }
        if (!logCommittables.isEmpty()) {
            List<LogCommT> ret = logCommitter.commit(logCommittables);
            if (!ret.isEmpty()) {
                return Collections.singletonList(new DynamicCommittable<>(ret, new HashMap<>()));
            }
        }

        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        this.logCommitter.close();
    }
}
