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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.mozilla.bagheera.cli.OptionFactory;

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
                    // TODO: use AWS builtin retry?
                    for (int i = 1; i <= retryCount; i++) {
                        if (upload(currentFile, true)) {
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

    public boolean upload(String filename, boolean delete) throws InterruptedException {
        File file = new File(filename);
        String keyName = file.getName();

        LOG.info("Preparing to upload " + filename + " as " + keyName);
        // TransferManager processes all transfers asynchronously,
        // so this call will return immediately.
        long start = System.currentTimeMillis();
        Upload upload = manager.upload(this.bucket, keyName, file);

        boolean success = false;
        try {
            LOG.info("Waiting for upload to complete...");
            // Block and wait for the upload to finish
            upload.waitForCompletion();
            double duration = (System.currentTimeMillis() - start) / 1000.0;
            double mbps = (file.length() / 1024.0 / 1024.0) / duration;
            LOG.info(String.format("Upload complete in %.02f seconds (%.02f MB/s).", duration, mbps));
            if (delete) {
                LOG.info("Deleting file.");
                if (file.delete()) {
                    LOG.info("File deleted successfully");
                } else {
                    LOG.info("Failed to delete " + filename);
                }
            }
            success = true;
        } catch (AmazonClientException e) {
            LOG.error("Unable to upload file " + filename + ", upload was aborted.", e);
        }
        return success;
    }

    public static TransferManager getManager() {
        ClientConfiguration config = new ClientConfiguration();
        String proxyHost = System.getProperty("http.proxyHost");
        if (proxyHost != null) {
            LOG.info("Setting proxy host: " + proxyHost);
            config.setProxyHost(proxyHost);
        }
        String proxyPort = System.getProperty("http.proxyPort");
        if (proxyPort != null) {
            LOG.info("Setting proxy port: " + proxyPort);
            config.setProxyPort(Integer.parseInt(proxyPort));
        }
        config.setMaxConnections(50);
        config.setMaxErrorRetry(5);
//        config.setConnectionTimeout(10);
//        config.setSocketTimeout(10);
//        TransferManagerConfiguration managerConfig = new TransferManagerConfiguration();
//        // Sets the minimum part size for upload parts.
//        managerConfig.setMinimumUploadPartSize(5 * Constants.MB);
//        // Sets the size threshold in bytes for when to use multipart uploads.
//        managerConfig.setMultipartUploadThreshold(10 * Constants.MB);

        AmazonS3Client awsClient = new AmazonS3Client(new ProfileCredentialsProvider(), config);

        TransferManager manager = new TransferManager(awsClient);
//        manager.setConfiguration(managerConfig);
        return manager;
    }

    public static void main(String[] args) throws ParseException {
        OptionFactory optFactory = OptionFactory.getInstance();
        Options options = new Options();
        options.addOption(optFactory.create("b", "bucket", true, "S3 bucket name").required());
        options.addOption(optFactory.create("f", "file", true, "File name").required());
        options.addOption(optFactory.create("d", "delete", false, "Delete file after successful upload?"));
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = parser.parse(options, args);

        String bucket = cmd.getOptionValue("bucket");
        String file = cmd.getOptionValue("file");
        boolean del = cmd.hasOption("delete");
        TransferManager manager = S3Loader.getManager();

        BlockingQueue<String> q = new ArrayBlockingQueue<String>(50);
        S3Loader loader = new S3Loader(q, manager, bucket);
        LOG.info("Preparing to upload");
        try {
            if (loader.upload(file, del)) {
                LOG.info("Successfully uploaded " + file);
            } else {
                LOG.error("Failed to upload " + file);
            }
        } catch (InterruptedException e) {
            LOG.error("Error uploading " + file, e);
        }
        manager.shutdownNow();
    }
}
