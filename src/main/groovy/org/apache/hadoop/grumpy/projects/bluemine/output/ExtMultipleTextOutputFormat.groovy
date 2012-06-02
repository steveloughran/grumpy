package org.apache.hadoop.grumpy.projects.bluemine.output

import groovy.util.logging.Commons
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

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
 * This class only works on the
 * @param < K >
 * @param < V >
 */
@Commons
class ExtMultipleTextOutputFormat<K, V> extends MultipleTextOutputFormat<K, V> implements ExtensionOptions {

    protected String extension = DEFAULT_EXTENSION;

    @Override
    protected String generateLeafFileName(String name) {
        return super.generateLeafFileName(name) + extension;
    }

    @Override
    protected String generateFileNameForKeyValue(K key, V value, String name) {
        return super.generateFileNameForKeyValue(key, value, name)
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf job)
    throws IOException {
        super.checkOutputSpecs(fileSystem, job);
        String ext = OutputUtils.getExtension(job)
        log.debug("extension is $ext")
        extension = ext;
    }
}