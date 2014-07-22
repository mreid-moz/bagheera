package com.mozilla.bagheera.sink;
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

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;

public class S3BatchSink extends BaseSink {
    private static final Logger LOG = Logger.getLogger(S3BatchSink.class);
    private long batchSize = 500000000l;
    private long currentBatch = 0l;
    private OutputStream outputStream = null;
    private final boolean compress;
    private final String bucket;

    StringBuilder builder = new StringBuilder(1024);

    public S3BatchSink(String s3Bucket, Long batchSize, Boolean useCompression) throws IOException {
        this.compress = useCompression;
        this.bucket = s3Bucket;
        this.outputStream = new BufferedOutputStream(new FileOutputStream(new File("/tmp/fhr.log")));
    }
    
    public long serialize(BagheeraMessage message, byte[] dataOverride) 
    	throws IOException {
    	byte[] data;
    	
    	if (dataOverride != null) {
    		data = dataOverride;
    	} else {
    		data = message.getPayload().toByteArray();
    	}
    	builder.setLength(0);
        builder.append("/submit/");
        builder.append(message.getNamespace());
        builder.append("/");
        builder.append(message.getId());
        for (String dim : message.getPartitionList()) {
            builder.append("/");
            builder.append(dim);
        }

        byte[] path = builder.toString().getBytes();

        if (this.compress) {
            ByteArrayOutputStream gzBytes = new ByteArrayOutputStream(data.length);
            try {
                GZIPOutputStream gzStream = new GZIPOutputStream(gzBytes);
                gzStream.write(data);
                gzStream.close();
            } finally {
                gzBytes.close();
            }
            data = gzBytes.toByteArray();
        }
        
        byte[] ip = message.getIpAddr().toByteArray();
        return writeRecord(ip, path, data, message.getTimestamp(), this.outputStream);
    }

    @Override
    public void store(BagheeraMessage message) throws IOException {
    	// If the current file is full, rotate and upload it.
    	if (currentBatch > batchSize) {
    		LOG.info("Rolling log after " + currentBatch + " bytes");
    		currentBatch = 0;
    	}
    	
    	// Convert the current message to incoming file format, and append
    	// it to the current file.
        long start = System.currentTimeMillis();
        serialize(message, null);
        long requestDuration = System.currentTimeMillis() - start;
    }
    
    public long writeRecord(byte[] ip, byte[] path, byte[] data, long timestamp, OutputStream outputStream) throws IOException {
    	// write out:
	    // Write record separator
	    writeUInt8(0x1e, outputStream);
	    
	    // Write client IP length
	    writeUInt8(ip.length, outputStream);

        // uint16 len_path
        writeUInt16LE(path.length, outputStream);

        // uint32 len_data
        writeUInt32LE(data.length, outputStream);

        // uint64 timestamp
        writeUInt64LE(timestamp, outputStream);
        
        // Write client ip
        outputStream.write(ip);
        // byte[] path
        outputStream.write(path);
        // byte[] data
        outputStream.write(data);
        
        return 16 + ip.length + path.length + data.length;
    }

    // Helpers:
    public void writeUInt8(int uint8, OutputStream stream) throws IOException {
        stream.write(uint8 & 0xFF);
    }
    public void writeUInt16LE(int uint16, OutputStream stream) throws IOException {
        writeUInt8(uint16, stream);
        writeUInt8(uint16 >> 8, stream);
    }
    public void writeUInt32LE(long uint32, OutputStream stream) throws IOException {
        writeUInt16LE((int)uint32 & 0x0000FFFF, stream);
        writeUInt16LE((int)(uint32 & 0xFFFF0000) >> 16, stream);
    }
    public void writeUInt64LE(long uint64, OutputStream stream) throws IOException {
        writeUInt32LE(uint64 & 0x00000000FFFFFFFF, stream);
        writeUInt32LE(uint64 >> 32, stream);
    }

    @Override
    public void close() {
        try {
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

	@Override
	public void store(String key, byte[] data, long timestamp) throws IOException {
		LOG.warn("Received store(key, data, ts) request");
	}

	@Override
	public void delete(BagheeraMessage message) throws IOException {
		serialize(message, "DELETE".getBytes());
	}
	
	@Override
	public void delete(String key) throws IOException {
		LOG.warn("Received delete(key) request");
	}

	@Override
	public void store(byte[] data) throws IOException {
		LOG.warn("Received store(data) request");
	}

	@Override
	public void store(String key, byte[] data) throws IOException {
		LOG.warn("Received store(key, data) request");
	}
}
