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
package com.mozilla.bagheera.util;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;

public class CompressionWorker implements Runnable {
    private final BlockingQueue<BagheeraMessage> pendingMessages;
    private final int workerId;
    private long batchSize = 500000000l;
    private final boolean compress;
    private static final Logger LOG = Logger.getLogger(CompressionWorker.class);
    private boolean done = false;
    private long currentBatch = 0l;
    private final BlockingQueue<String> completedFiles;
    private final String outputPath;
    private OutputStream outputStream = null;
    StringBuilder builder = new StringBuilder(1024);

    public CompressionWorker(BlockingQueue<BagheeraMessage> pendingMessages, int workerId, Long batchSize,
            Boolean useCompression, String outputPath, BlockingQueue<String> completedFiles) {
        this.pendingMessages = pendingMessages;
        this.workerId = workerId;
        this.batchSize = batchSize;
        this.compress = useCompression;
        this.outputPath = outputPath;
        this.completedFiles = completedFiles;
        try {
            this.outputStream = new BufferedOutputStream(new FileOutputStream(new File(getLogFilename())));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            LOG.fatal("Worker failed to open file output stream:" + getLogFilename(), e);
        }
    }

    // We're done when we've been told *and* when all pending uploads are complete.
    public boolean done() {
        return this.done && this.pendingMessages.isEmpty();
    }

    public void stop() {
        LOG.info("Received stop message. Processing any remaining queue items, then shutting down...");
        this.done = true;
    }

    @Override
    public void run() {
        while (!done()) {
            try {
                BagheeraMessage currentMessage = pendingMessages.poll(3, TimeUnit.SECONDS);
                if (currentMessage != null) {
                    try {
                        serialize(currentMessage);
                    } catch (IOException e) {
                        LOG.error("Error serializing message", e);
                    }
                }
            } catch (InterruptedException ex) {
                LOG.info("Worker interrupted during polling", ex);
                //TODO: ... handle ...
            }
        }
        LOG.info("All done. Rotating final log");


        try {
            String rotatedLogName = rotateLog();
            LOG.info("Queueing upload: " + rotatedLogName);
            completedFiles.put(rotatedLogName);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            LOG.error("Error queueing upload", e);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            LOG.error("Error rotating final output file for worker " + workerId, e);
        }
    }

    public String getLogFilename() {
        return this.outputPath + "/fhr." + this.workerId + ".log";
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

    public byte[] compress(byte[] data) throws IOException {
        ByteArrayOutputStream gzBytes = new ByteArrayOutputStream(data.length);
        try {
            GZIPOutputStream gzStream = new GZIPOutputStream(gzBytes);
            gzStream.write(data);
            gzStream.close();
        } finally {
            gzBytes.close();
        }
        data = gzBytes.toByteArray();
        return data;
    }

    public long serialize(BagheeraMessage message) throws IOException {
        // If the current file is full, rotate and upload it.
        if (currentBatch > batchSize) {
            LOG.info("Preparing to rotate the current log...");
            LOG.info("Rotating log after " + currentBatch + " bytes");
            try {
                String rotatedLogName = rotateLog();
                // Only if it was successful should we update currentBatch size.  If we failed to move,
                // we should try again on the next write, I guess...
                currentBatch = 0;

                try {
                    LOG.info("Queueing upload: " + rotatedLogName);
                    completedFiles.put(rotatedLogName);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    LOG.error("Error queueing upload", e);
                }
            } catch (IOException e) {
                LOG.error("Error rotating file", e);
            }
            // now reopen the outputStream
            this.outputStream = new BufferedOutputStream(new FileOutputStream(new File(getLogFilename())));
        }

        byte[] data = message.getPayload().toByteArray();

        if (this.compress) {
            data = compress(data);
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

        currentRecordLength = writeRecord(ip, path, data, message.getTimestamp(), this.outputStream);
        currentBatch += currentRecordLength;
        return currentRecordLength;
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
}
