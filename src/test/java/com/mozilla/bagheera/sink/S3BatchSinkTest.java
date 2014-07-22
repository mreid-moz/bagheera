/*
 * Copyright 2013 Mozilla Foundation
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

// Don't care that this httpserver stuff is restricted.
@SuppressWarnings("restriction")
public class S3BatchSinkTest {

    @Test
    public void testWriteRecord() throws IOException{
        S3BatchSink sink = new S3BatchSink("test", 100l, false);
        
        byte[] ip = "123.123.123.123".getBytes();
        byte[] path = "/submit/test".getBytes();
        byte[] data = "{\"test\": 1}".getBytes();
        long timestamp = 1000000l;
        
        ByteArrayOutputStream testOut = new ByteArrayOutputStream(1500);
        long length = sink.writeRecord(ip, path, data, timestamp, testOut);
        testOut.close();
        long expectedLength = 16 + ip.length + path.length + data.length;
        assertEquals(expectedLength, length);
        byte[] written = testOut.toByteArray();
        for (int i = 0; i < ip.length; i++) {
            assertEquals(written[16 + i], ip[i]);
        }
        for (int i = 0; i < path.length; i++) {
            assertEquals(written[16 + ip.length + i], path[i]);
        }
        for (int i = 0; i < data.length; i++) {
            assertEquals(written[16 + ip.length + path.length + i], data[i]);
        }
        
        sink.close();
    }
}
