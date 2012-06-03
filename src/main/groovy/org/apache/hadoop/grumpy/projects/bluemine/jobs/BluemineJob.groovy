package org.apache.hadoop.grumpy.projects.bluemine.jobs

import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.grumpy.GrumpyJob
import org.apache.hadoop.grumpy.Keys
import org.apache.hadoop.grumpy.projects.bluemine.events.BlueEvent
import org.apache.hadoop.grumpy.projects.bluemine.output.ExtTextOutputFormat
import org.apache.hadoop.grumpy.projects.bluemine.output.ExtensionOptions

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf

import org.apache.hadoop.grumpy.tools.GrumpyUtils

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
class BluemineJob extends GrumpyJob {

  BluemineJob(String jobName) {
    super(jobName)
  }

  BluemineJob(Configuration conf) {
    super(conf)
  }

  BluemineJob(Configuration conf, String jobName) {
    super(conf, jobName)
  }

  @Override
  String toString() {
    StringBuilder builder = new StringBuilder(200)
    builder.append(getClass().name).append(": ").append(jobName).append("\n")
    appendConf(builder, ["mapred.input.dir", "mapred.output.dir", "mapred.job.tracker"])
    appendConf(builder, Keys.JOB_KEY_JARS)
    appendConf(builder, Keys.MAP_KEY_CLASS)
    appendConf(builder, org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY)
    appendConf(builder, [])
    appendConf(builder, OUTPUT_FORMAT_CLASS_ATTR)
    appendPattern(builder, "bluemine.")
    builder.toString()
  }

  protected void appendConf(StringBuilder builder, List<String> keys) {
    keys.each() { key ->
      appendKV(builder, key, get(key))
    }
  }

  protected void appendConf(StringBuilder builder, String key) {
    appendKV(builder, key, get(key))
  }

  protected void appendKV(StringBuilder builder, String key, String value) {
    builder.append(key).append("=\"").append(value).append('"\n')
  }

  protected void appendPattern(StringBuilder builder, String pattern) {
    conf.each() { entry ->
      if (entry.key.startsWith(pattern)) {
        appendKV(builder, entry.key, entry.value)
      }
    }
  }

  void makeMapEmitEvents() {
    mapOutputKeyClass = Text
    mapOutputValueClass = BlueEvent
    outputCSVFiles()
  }

  void outputCSVFiles() {
    set(ExtensionOptions.KEY_EXTENSION, ".csv")
    set(ExtensionOptions.KEY_SEPARATOR, ",")
    set(OUTPUT_FORMAT_CLASS_ATTR, ExtTextOutputFormat.name)
  }

  /**
   * Create a basic job with the given M & R jobs. 
   * The Groovy JAR is added as another needed JAR; the mapClass is set as the main jar of the job
   * @param name job name
   * @param conf configuration
   * @param mapClass mapper
   * @param reduceClass reducer
   * @return
   */
  static BluemineJob createBasicJob(String name,
                                    JobConf conf,
                                    Class mapClass,
                                    Class reduceClass) {

    BluemineJob job = new BluemineJob(conf, name)

    job.addGroovyJar();
    log.info(" map class is $mapClass reduce class is $reduceClass")
    String jar = GrumpyUtils.findContainingJar(mapClass)
    log.info(" map class is at $jar")
    job.jarByClass = mapClass
    job.mapperClass = mapClass
    job.reducerClass = reduceClass
    //set up csv output 
    job.outputCSVFiles();
    job
  }
}
