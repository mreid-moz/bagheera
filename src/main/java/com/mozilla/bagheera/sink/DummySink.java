/*
 * Copyright 2012 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.bagheera.sink;

import java.io.IOException;

import org.apache.log4j.Logger;

public class DummySink extends BaseSink {

    private static final Logger LOG = Logger.getLogger(DummySink.class);
    private long updateCount = 0;
    private long deleteCount = 0;
    private final int batchSize = 100000;

    public DummySink(SinkConfiguration sinkConfiguration) {
    }

    public void logProgress() {
        LOG.info(String.format("Processed %d: %d updates, %d deletes", (updateCount + deleteCount), updateCount, deleteCount));
    }

    @Override
    public void close() {
        logProgress();
    }

    @Override
    public void store(byte[] data) throws IOException {
        incrementUpdates();
    }

    private void incrementUpdates() {
        updateCount += 1;
        if ((updateCount + deleteCount) % batchSize == 0) {
            logProgress();
        }
    }

    @Override
    public void store(String key, byte[] data) throws IOException {
        incrementUpdates();
    }

    @Override
    public void store(String key, byte[] data, long timestamp) throws IOException {
        incrementUpdates();
    }

    @Override
    public void delete(String key) {
        deleteCount += 1;
        if ((updateCount + deleteCount) % batchSize == 0) {
            logProgress();
        }
    }
}
