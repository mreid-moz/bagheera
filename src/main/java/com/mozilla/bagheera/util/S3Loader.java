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

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

public class S3Loader implements Runnable {
    private final BlockingQueue<String> pendingUploads;
    private final TransferManager manager;
    private final String bucket;
    private int retryCount = 3;
    private static final Logger LOG = Logger.getLogger(S3Loader.class);
    private boolean done = false;

    public S3Loader(BlockingQueue<String> pendingUploads, TransferManager manager, String bucket) {
        this.pendingUploads = pendingUploads;
        this.manager = manager;
        this.bucket = bucket;
    }

    // We're done when we've been told *and* when all pending uploads are complete.
    public boolean done() {
        return this.done && this.pendingUploads.isEmpty();
    }

    public void stop() {
        LOG.info("Received stop message. Processing any remaining queue items, then shutting down...");
        this.done = true;
    }

    @Override
    public void run() {
        while (!done()) {
            try {
                String currentFile = pendingUploads.poll(3, TimeUnit.SECONDS);
                if (currentFile != null) {
                    for (int i = 1; i <= retryCount; i++) {
                        if (upload(currentFile)) {
                            break;
                        } else {
                            if (i == retryCount) {
                                LOG.error("All upload attempts failed for file " + currentFile);
                            } else {
                                LOG.warn("Upload attempt " + i + " failed for file " + currentFile);
                            }
                        }
                    }
                }
            } catch (InterruptedException ex) {
                //TODO: ... handle ...
            }
        }
    }

    public boolean upload(String filename) throws InterruptedException {
        File file = new File(filename);
        String keyName = file.getName();

        LOG.info("Preparing to upload " + filename + " as " + keyName);
        // TransferManager processes all transfers asynchronously,
        // so this call will return immediately.
        Upload upload = manager.upload(this.bucket, keyName, file);

        boolean success = false;
        try {
            LOG.info("Waiting for upload to complete...");
            // Or you can block and wait for the upload to finish
            upload.waitForCompletion();
            LOG.info("Upload complete. Deleting file.");
            if (file.delete()) {
                LOG.info("File deleted successfully");
            } else {
                LOG.info("Failed to delete " + filename);
            }
            success = true;
        } catch (AmazonClientException e) {
            LOG.error("Unable to upload file " + filename + ", upload was aborted.", e);
        }
        return success;
    }
}
