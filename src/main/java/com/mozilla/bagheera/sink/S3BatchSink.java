package com.mozilla.bagheera.sink;
/*
 * Copyright 2014 Mozilla Foundation
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;
import com.mozilla.bagheera.util.S3Loader;

public class S3BatchSink extends BaseSink {
    private static final Logger LOG = Logger.getLogger(S3BatchSink.class);
    private long batchSize = 500000000l;
    private long currentBatch = 0l;
    private OutputStream outputStream = null;
    private final boolean compress;
    private final String bucket;
    private final String outputPath;
    private final Lock lock = new ReentrantLock();
    // We probably don't want more than 50 files queued up at once...
    private final BlockingQueue<String> s3queue = new ArrayBlockingQueue<String>(50);
    private final S3Loader loader;
    private final Thread loaderThread;

    StringBuilder builder = new StringBuilder(1024);

    public S3BatchSink(SinkConfiguration sinkConfiguration) throws IOException {
        this(sinkConfiguration.getString("s3batchsink.bucket"),
                sinkConfiguration.getString("s3batchsink.path"),
                sinkConfiguration.getLong("s3batchsink.size"),
                sinkConfiguration.getBoolean("s3batchsink.compress"));
    }

    public S3BatchSink(String s3Bucket, String outputPath, Long batchSize, Boolean useCompression) throws IOException {
        this.outputPath = outputPath;
        this.batchSize = batchSize;
        this.compress = useCompression;
        this.bucket = s3Bucket;
        this.outputStream = new BufferedOutputStream(new FileOutputStream(new File(getLogFilename())));
        TransferManager manager = S3Loader.getManager();
        loader = new S3Loader(s3queue, manager, this.bucket);
        loaderThread = new Thread(loader);
        loaderThread.start();
    }

    public String getLogFilename() {
        return this.outputPath + "/fhr.log";
    }

    public long serialize(BagheeraMessage message, byte[] dataOverride) throws IOException {
        // If the current file is full, rotate and upload it.
        if (currentBatch > batchSize) {
            LOG.info("Waiting for lock to rotate...");
            lock.lock();
            try {
                LOG.info("Rotating log after " + currentBatch + " bytes");
                try {
                    String rotatedLogName = rotateLog();
                    // Only if it was successful should we update currentBatch size.  If we failed to move,
                    // we should try again on the next write, I guess...
                    currentBatch = 0;

                    try {
                        LOG.info("Queueing upload: " + rotatedLogName);
                        s3queue.put(rotatedLogName);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        LOG.error("Error queueing upload", e);
                    }
                } catch (IOException e) {
                    LOG.error("Error rotating file", e);
                }
                // now reopen the outputStream
                this.outputStream = new BufferedOutputStream(new FileOutputStream(new File(getLogFilename())));
                // TODO: Enqueue the rotated log for S3 upload.
            } finally {
                LOG.info("Unlocking...");
                lock.unlock();
            }
        }

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
        byte[] ip = message.getIpAddr().toByteArray();
        long currentRecordLength = -1l;

        // Obtain a lock before writing to make sure we don't write
        // while we're rotating.
        lock.lock();
        try {
            currentRecordLength = writeRecord(ip, path, data, message.getTimestamp(), this.outputStream);
        } finally {
            lock.unlock();
        }
        currentBatch += currentRecordLength;
        return currentRecordLength;
    }

    public String rotateLog() throws IOException {
        // Prepare to rotate by closing the file.
        this.outputStream.flush();
        this.outputStream.close();

        String mainLogName = getLogFilename();
        String rotatedLogName = mainLogName + "." + System.currentTimeMillis();
        LOG.info("Moving " + mainLogName + " to " + rotatedLogName);
        try {
            Files.move(Paths.get(mainLogName), Paths.get(rotatedLogName), StandardCopyOption.ATOMIC_MOVE);
            LOG.info("Success!");
        } catch (IOException e) {
            LOG.error("Error rotating file", e);
            throw e;
        }
        return rotatedLogName;
    }

    @Override
    public void store(BagheeraMessage message) throws IOException {

    	// Convert the current message to incoming file format, and append
    	// it to the current file.
        long start = System.currentTimeMillis();
        serialize(message, null);
        long requestDuration = System.currentTimeMillis() - start;
    }

    @Override
    public void delete(BagheeraMessage message) throws IOException {
        serialize(message, "DELETE".getBytes());
    }

    public long writeRecord(byte[] ip, byte[] path, byte[] data, long timestamp, OutputStream outputStream) throws IOException {
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
            // TODO: rotate final file
            //       add to queue

            loader.stop();
            // Wait for loader to complete.
            loaderThread.join();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO
            e.printStackTrace();
        }
    }

	@Override
	public void store(String key, byte[] data, long timestamp) throws IOException {
		LOG.warn("Received store(key, data, ts) request");
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
