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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Logger;

import com.amazonaws.services.s3.transfer.TransferManager;
import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;
import com.mozilla.bagheera.util.CompressionWorker;
import com.mozilla.bagheera.util.S3Loader;

public class S3BatchSink extends BaseSink {
    private static final Logger LOG = Logger.getLogger(S3BatchSink.class);
//    private long batchSize = 500000000l;
    private final int numParallelLoaders = 4;
    private final int numCompressionWorkers = 4;
//    private long currentBatch = 0l;
//    private OutputStream outputStream = null;
//    private final boolean compress;
//    private final Lock lock = new ReentrantLock();
    // We probably don't want more than 50 files queued up at once...
    private final BlockingQueue<String> s3queue = new ArrayBlockingQueue<String>(50);
    private final BlockingQueue<BagheeraMessage> messageQueue = new ArrayBlockingQueue<BagheeraMessage>(1000);
    private final List<S3Loader> loaders;
    private final List<Thread> loaderThreads;
    private final List<CompressionWorker> workers;
    private final List<Thread> workerThreads;

//    StringBuilder builder = new StringBuilder(1024);

    public S3BatchSink(SinkConfiguration sinkConfiguration) throws IOException {
        this(sinkConfiguration.getString("s3batchsink.bucket"),
                sinkConfiguration.getString("s3batchsink.path"),
                sinkConfiguration.getLong("s3batchsink.size"),
                sinkConfiguration.getBoolean("s3batchsink.compress"));
    }

    public S3BatchSink(String s3Bucket, String outputPath, Long batchSize, Boolean useCompression) throws IOException {
        TransferManager manager = S3Loader.getManager();
        loaders = new ArrayList<S3Loader>();
        loaderThreads = new ArrayList<Thread>();
        workers = new ArrayList<CompressionWorker>();
        workerThreads = new ArrayList<Thread>();
        for (int i = 0; i < numParallelLoaders; i++) {
            S3Loader loader = new S3Loader(s3queue, manager, s3Bucket);
            Thread loaderThread = new Thread(loader);
            loaderThread.start();
            loaders.add(loader);
            loaderThreads.add(loaderThread);
        }

        for (int i = 0; i < numCompressionWorkers; i++) {
            CompressionWorker worker = new CompressionWorker(messageQueue, i, batchSize, useCompression, outputPath, s3queue);
            Thread workerThread = new Thread(worker);
            workerThread.start();
            workers.add(worker);
            workerThreads.add(workerThread);
        }
    }

//    public String getLogFilename() {
//        return this.outputPath + "/fhr.log";
//    }

//    public long serialize(BagheeraMessage message, byte[] dataOverride) throws IOException {
//        // If the current file is full, rotate and upload it.
//        if (currentBatch > batchSize) {
//            LOG.info("Waiting for lock so we can safely rotate the current log...");
//            lock.lock();
//            try {
//                LOG.info("Rotating log after " + currentBatch + " bytes");
//                try {
//                    String rotatedLogName = rotateLog();
//                    // Only if it was successful should we update currentBatch size.  If we failed to move,
//                    // we should try again on the next write, I guess...
//                    currentBatch = 0;
//
//                    try {
//                        LOG.info("Queueing upload: " + rotatedLogName);
//                        s3queue.put(rotatedLogName);
//                    } catch (InterruptedException e) {
//                        // TODO Auto-generated catch block
//                        LOG.error("Error queueing upload", e);
//                    }
//                } catch (IOException e) {
//                    LOG.error("Error rotating file", e);
//                }
//                // now reopen the outputStream
//                this.outputStream = new BufferedOutputStream(new FileOutputStream(new File(getLogFilename())));
//                // TODO: Enqueue the rotated log for S3 upload.
//            } finally {
//                LOG.info("Unlocking...");
//                lock.unlock();
//            }
//        }
//
//    	byte[] data;
//    	if (dataOverride != null) {
//    		data = dataOverride;
//    	} else {
//    		data = message.getPayload().toByteArray();
//    	}
//
//        if (this.compress) {
//            data = compress(data);
//        }
//
//    	builder.setLength(0);
//        builder.append("/submit/");
//        builder.append(message.getNamespace());
//        builder.append("/");
//        builder.append(message.getId());
//        for (String dim : message.getPartitionList()) {
//            builder.append("/");
//            builder.append(dim);
//        }
//
//        byte[] path = builder.toString().getBytes();
//        byte[] ip = message.getIpAddr().toByteArray();
//        long currentRecordLength = -1l;
//
//        // Obtain a lock before writing to make sure we don't write
//        // while we're rotating.
//        lock.lock();
//        try {
//            currentRecordLength = writeRecord(ip, path, data, message.getTimestamp(), this.outputStream);
//        } finally {
//            lock.unlock();
//        }
//        currentBatch += currentRecordLength;
//        return currentRecordLength;
//    }

//    public byte[] compress(byte[] data) throws IOException {
//        ByteArrayOutputStream gzBytes = new ByteArrayOutputStream(data.length);
//        try {
//            GZIPOutputStream gzStream = new GZIPOutputStream(gzBytes);
//            gzStream.write(data);
//            gzStream.close();
//        } finally {
//            gzBytes.close();
//        }
//        data = gzBytes.toByteArray();
//        return data;
//    }

//    public String rotateLog() throws IOException {
//        // Prepare to rotate by closing the file.
//        this.outputStream.flush();
//        this.outputStream.close();
//
//        String mainLogName = getLogFilename();
//        String rotatedLogName = mainLogName + "." + System.currentTimeMillis();
//        LOG.info("Moving " + mainLogName + " to " + rotatedLogName);
//        try {
//            Files.move(Paths.get(mainLogName), Paths.get(rotatedLogName), StandardCopyOption.ATOMIC_MOVE);
//            LOG.info("Success!");
//        } catch (IOException e) {
//            LOG.error("Error rotating file", e);
//            throw e;
//        }
//        return rotatedLogName;
//    }

    @Override
    public void store(BagheeraMessage message) throws IOException {
        try {
            messageQueue.put(message);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            LOG.error("Error queueing message", e);
        }
    }

    @Override
    public void delete(BagheeraMessage message) throws IOException {
        store(message);
    }

//    public long writeRecord(byte[] ip, byte[] path, byte[] data, long timestamp, OutputStream outputStream) throws IOException {
//    	// write out:
//	    // Write record separator
//	    writeUInt8(0x1e, outputStream);
//
//	    // Write client IP length
//	    writeUInt8(ip.length, outputStream);
//
//        // uint16 len_path
//        writeUInt16LE(path.length, outputStream);
//
//        // uint32 len_data
//        writeUInt32LE(data.length, outputStream);
//
//        // uint64 timestamp
//        writeUInt64LE(timestamp, outputStream);
//
//        // Write client ip
//        outputStream.write(ip);
//        // byte[] path
//        outputStream.write(path);
//        // byte[] data
//        outputStream.write(data);
//
//        return 16 + ip.length + path.length + data.length;
//    }
//
//    // Helpers:
//    public void writeUInt8(int uint8, OutputStream stream) throws IOException {
//        stream.write(uint8 & 0xFF);
//    }
//    public void writeUInt16LE(int uint16, OutputStream stream) throws IOException {
//        writeUInt8(uint16, stream);
//        writeUInt8(uint16 >> 8, stream);
//    }
//    public void writeUInt32LE(long uint32, OutputStream stream) throws IOException {
//        writeUInt16LE((int)uint32 & 0x0000FFFF, stream);
//        writeUInt16LE((int)(uint32 & 0xFFFF0000) >> 16, stream);
//    }
//    public void writeUInt64LE(long uint64, OutputStream stream) throws IOException {
//        writeUInt32LE(uint64 & 0x00000000FFFFFFFF, stream);
//        writeUInt32LE(uint64 >> 32, stream);
//    }

    @Override
    public void close() {
        LOG.debug("Closing S3BatchSink");
        try {
            // Wait for workers to complete.
            for (CompressionWorker worker : workers) {
                worker.stop();
            }
            for (Thread workerThread : workerThreads) {
                workerThread.join();
            }

            // Wait for loaders to complete.
            for (S3Loader loader : loaders) {
                loader.stop();
            }
            for (Thread loaderThread : loaderThreads) {
                loaderThread.join();
            }
        } catch (InterruptedException e) {
            // TODO
            LOG.error("Interrupted during shutdown.", e);
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
