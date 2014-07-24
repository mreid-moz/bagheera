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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import org.junit.Test;

public class S3BatchSinkTest {

    @Test
    public void testWriteRecord() throws IOException{
        S3BatchSink sink = new S3BatchSink("test", ".", 100l, false);

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

    @Test
    public void testAssignability() {
        assertTrue(KeyValueSink.class.isAssignableFrom(S3BatchSink.class));
    }

    @Test
    public void testWriteRecordCompressed() throws IOException{
        S3BatchSink sink = new S3BatchSink("test", ".", 100l, true);

        byte[] ip = "123.123.123.123".getBytes();
        byte[] path = "/submit/test".getBytes();

        StringBuilder builder = new StringBuilder();
        builder.append("{\"test0\": 0");
        for (int i = 1; i < 500; i++) {
            builder.append(",\"test");
            builder.append(i);
            builder.append("\":");
            builder.append(i);
        }
        builder.append("}");
        byte[] data = builder.toString().getBytes();
        long timestamp = 1000000l;

        ByteArrayOutputStream testOut = new ByteArrayOutputStream(50000);
        long length = sink.writeRecord(ip, path, data, timestamp, testOut);
        testOut.close();
        int expectedLength = 16 + ip.length + path.length + data.length;

        //System.out.println("Raw length: " + expectedLength + ", compressed length: " + length);
        assertTrue(expectedLength > length);
        byte[] written = testOut.toByteArray();
        for (int i = 0; i < ip.length; i++) {
            assertEquals(written[16 + i], ip[i]);
        }
        for (int i = 0; i < path.length; i++) {
            assertEquals(written[16 + ip.length + i], path[i]);
        }
        // Don't check data yet, since it's all scrambled up.
        byte[] gzData = Arrays.copyOfRange(written, 16 + ip.length + path.length, written.length);
        ByteArrayInputStream gzBytes = new ByteArrayInputStream(gzData);
        byte[] uncompressed = new byte[data.length];
        try {
            GZIPInputStream gzStream = new GZIPInputStream(gzBytes);
            // This is a stupid way to read it, but reading "uncompressed"
            // all at once doesn't seem to work very well.
            int bytesRead = 0;
            int currentByte = gzStream.read();
            while (currentByte != -1) {
                uncompressed[bytesRead++] = (byte)currentByte;
                if (bytesRead > uncompressed.length) {
                    System.out.println("Too many bytes!");
                    break;
                }
                currentByte = gzStream.read();
            }
            assertEquals(data.length, bytesRead);
            gzStream.close();
        } finally {
            gzBytes.close();
        }

        // Check that they're the same
        for (int i = 0; i < data.length; i++) {
            assertEquals(uncompressed[i], data[i]);
        }

        sink.close();
    }
}
