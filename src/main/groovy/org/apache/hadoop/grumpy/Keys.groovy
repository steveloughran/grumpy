/*
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

package org.apache.hadoop.grumpy

/**
 *
 */
public interface Keys {
    String TEST_DATA_DIR = "test.build.data"
    String HADOOP_LOG_DIR = "hadoop.log.dir"

    String JOB_KEY_FILES = "tmpfiles";

    /** comma separated list of JARS that are uploaded to the distributed cache on job submission. 
     * No spaces before/after filesnames.
     * @see org.apache.hadoop.mapred.JobClient#copyAndConfigureFiles
     * */
    String JOB_KEY_JARS = "tmpjars";
    /** comma separated list of archive files */
    String JOB_KEY_ARCHIVES = "tmparchives";
    /** csv of file location in the remote fs */
    String JOB_KEY_ARCHIVE_CACHE = "mapred.cache.archives"
    String MAP_KEY_CLASS = "mapred.mapoutput.key.class"
    String MAPRED_INPUT_DIR = "mapred.input.dir"
    String MAPRED_OUTPUT_DIR = "mapred.output.dir"
    String MAPRED_DISABLE_TOOL_WARNING = "mapred.used.genericoptionsparser";
    String FS_DEFAULT_NAME_KEY = org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
}