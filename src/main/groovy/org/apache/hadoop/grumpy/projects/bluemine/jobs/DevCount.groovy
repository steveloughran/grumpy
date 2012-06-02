package org.apache.hadoop.grumpy.projects.bluemine.jobs

import groovy.util.logging.Commons
import org.apache.hadoop.grumpy.projects.bluemine.mr.DeviceCountMap
import org.apache.hadoop.grumpy.projects.bluemine.reducers.CountReducer
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf

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
@Commons
class DevCount extends BlueMain {

  static void main(String[] args) {
    BlueMain main = new DevCount()
    executeAndExit(main, args)
  }


  @Override
  String getName() {"DevCount"}

  @Override
  protected boolean execute(String[] args) {
    OptionAccessor options = parseCommandLine(args)
    if (options == null) {
      return false
    }
    JobConf conf = new JobConf()
    setTrackerURL(conf, options)
    setFilesystemURL(conf, options)
    loadProperties(conf, options)
    BluemineJob job = BluemineJob.createBasicJob("devcount",
                                                 conf,
                                                 DeviceCountMap,
                                                 CountReducer)
    job.combinerClass = CountReducer
    job.mapOutputKeyClass = Text
    job.mapOutputValueClass = IntWritable
    return bindAndExecute(options, job)
  }

}
