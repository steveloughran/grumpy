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

package org.apache.hadoop.grumpy.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.grumpy.Keys
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.hdfs.server.common.HdfsConstants
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.MiniMRCluster

class MiniClusters {

  public static final String HADOOP_LOG_DIR = "hadoop.log.dir"



  static MiniMRCluster createMiniMRCluster(int numTaskTrackers,
                                        String fsURI,
                                        int numDir,
                                        JobConf conf) throws IOException {
    Sysprops[HADOOP_LOG_DIR] =
      conf.get(HADOOP_LOG_DIR) ?: Sysprops["java.io.tmpdir"];
    new MiniMRCluster(numTaskTrackers, fsURI, numDir, null, null, conf)
  }


  /**
 * fix the problem with MiniDFS clusters being system property driven
 * @param conf config file
 */
  static MiniDFSCluster createMiniDFSCluster(Configuration conf,
                                       int numDataNodes) {
    Sysprops[Keys.TEST_DATA_DIR] = conf.get(Keys.TEST_DATA_DIR)
    new MiniDFSCluster(conf, numDataNodes, HdfsConstants.StartupOption.FORMAT)
  }
}
