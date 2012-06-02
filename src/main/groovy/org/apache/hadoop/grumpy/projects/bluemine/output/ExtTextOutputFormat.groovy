package org.apache.hadoop.grumpy.projects.bluemine.output

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util.ReflectionUtils

import groovy.util.logging.Commons

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This is a New API  output formatter. Mostly a copy and paste from the ASF original, with some
 * tweaks for groovy and a controllable extension option
 */
@Commons
class ExtTextOutputFormat<K, V> extends TextOutputFormat<K, V> implements ExtensionOptions {

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        String keyValueSeparator = conf.get(KEY_SEPARATOR, "\t");
        CompressionCodec codec = null;
        String codecExt = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass =
                getOutputCompressorClass(job, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            codecExt = codec.getDefaultExtension();
            log.info("Compressing with $codecClass extension=$codecExt")
        }
        String csv = OutputUtils.getExtension(conf)
        String extension = csv + codecExt
        log.info("Output extension =$extension")
        Path file = getDefaultWorkFile(job, extension);
        FileSystem fs = file.getFileSystem(conf);
        if (!isCompressed) {
            FSDataOutputStream fileOut = fs.create(file, false);
            return new TextOutputFormat.LineRecordWriter<K, V>(fileOut, keyValueSeparator);
        } else {
            FSDataOutputStream fileOut = fs.create(file, false);
            return new TextOutputFormat.LineRecordWriter<K, V>(new DataOutputStream
            (codec.createOutputStream(fileOut)),
                    keyValueSeparator);
        }
    }
}
