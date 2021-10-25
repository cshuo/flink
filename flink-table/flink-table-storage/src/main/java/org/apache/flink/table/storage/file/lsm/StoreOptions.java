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

import java.io.Serializable;

/** */
public class StoreOptions implements Serializable {

    public final int numFilesLevel0;
    public final int numLevels;
    public final long targetFileSize;
    public final long minFileSize;
    public final int maxSizeAmplificationPercent;
    public final int sizeRatio;
    public final boolean commitForceCompact;

    // TODO replace maxMemRecords with maxMemBytes
    public final int maxMemRecords = 10_000;

    public StoreOptions(
            int numFilesLevel0,
            int numLevels,
            long targetFileSize,
            long minFileSize,
            int maxSizeAmplificationPercent,
            int sizeRatio,
            boolean commitForceCompact) {
        this.numFilesLevel0 = numFilesLevel0;
        this.numLevels = numLevels;
        this.targetFileSize = targetFileSize;
        this.minFileSize = minFileSize;
        this.maxSizeAmplificationPercent = maxSizeAmplificationPercent;
        this.sizeRatio = sizeRatio;
        this.commitForceCompact = commitForceCompact;
    }
}
