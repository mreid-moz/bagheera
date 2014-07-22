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
import java.util.UUID;

import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;
import com.mozilla.bagheera.BagheeraProto.BagheeraMessage.Operation;

public abstract class BaseSink implements Sink, KeyValueSink {

	@Override
	public void store(BagheeraMessage bmsg) throws IOException {
        if (bmsg.hasTimestamp()) {
            this.store(bmsg.getId(), bmsg.getPayload().toByteArray(), bmsg.getTimestamp());
        } else {
            this.store(bmsg.getId(), bmsg.getPayload().toByteArray());
        }
	}

	@Override
	public void delete(BagheeraMessage bmsg) throws IOException {
		// Assume we're only getting valid messages.
		if (bmsg.getOperation() == Operation.CREATE_UPDATE) {
            this.delete(bmsg.getId());
		}
	}

	@Override
	public void store(byte[] data) throws IOException {
		// Assign a key.
		this.store(UUID.randomUUID().toString(), data);
	}
}
