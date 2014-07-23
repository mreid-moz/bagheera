package com.mozilla.bagheera.consumer;
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


import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import com.mozilla.bagheera.cli.App;
import com.mozilla.bagheera.cli.OptionFactory;
import com.mozilla.bagheera.sink.KeyValueSinkFactory;
import com.mozilla.bagheera.sink.SinkConfiguration;
import com.mozilla.bagheera.sink.S3BatchSink;
import com.mozilla.bagheera.util.ShutdownHook;

/**
 * Kafka consumer which reads from a kafka queue, batches requests into blobs, and uploads them to S3.
 */
public final class S3BatchConsumer extends App {

    private static final Logger LOG = Logger.getLogger(S3BatchConsumer.class);

    public static void main(String[] args) {
        OptionFactory optFactory = OptionFactory.getInstance();
        Options options = KafkaConsumer.getOptions();
        options.addOption(optFactory.create("b", "bucket", true, "S3 bucket name").required());
        options.addOption(optFactory.create("s", "size", true, "Batch size in bytes"));
        // TODO: AWS credentials?
        options.addOption(optFactory.create("z", "compress-payloads", true, "Whether or not to gzip payload data"));


        CommandLineParser parser = new GnuParser();
        ShutdownHook sh = ShutdownHook.getInstance();
        try {
            // Parse command line options
            CommandLine cmd = parser.parse(options, args);

            final KafkaConsumer consumer = KafkaConsumer.fromOptions(cmd);
            sh.addFirst(consumer);

            // Create a sink for storing data
            SinkConfiguration sinkConfig = new SinkConfiguration();
            if (cmd.hasOption("numthreads")) {
                sinkConfig.setInt("hbasesink.hbase.numthreads", Integer.parseInt(cmd.getOptionValue("numthreads")));
            }
            sinkConfig.setString("s3batchsink.bucket", cmd.getOptionValue("bucket", "mreid-fhr-incoming"));
            sinkConfig.setString("s3batchsink.size", cmd.getOptionValue("size", "500000000"));
            sinkConfig.setString("s3batchsink.compress", cmd.getOptionValue("compress-payloads", "false"));
            KeyValueSinkFactory sinkFactory = KeyValueSinkFactory.getInstance(S3BatchSink.class, sinkConfig);
            sh.addLast(sinkFactory);

            // Set the sink factory for consumer storage
            consumer.setSinkFactory(sinkFactory);

            prepareHealthChecks();

            // Initialize metrics collection, reporting, etc.
//            final MetricsManager manager = MetricsManager.getDefaultMetricsManager();

            // Begin polling
            consumer.poll();
        } catch (ParseException e) {
            LOG.error("Error parsing command line options", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(S3BatchConsumer.class.getName(), options);
        }
    }
}
