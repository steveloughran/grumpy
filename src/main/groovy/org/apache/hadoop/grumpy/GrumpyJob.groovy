/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.hadoop.grumpy

import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.grumpy.output.ExtensionOptions
import org.apache.hadoop.grumpy.tools.GrumpyUtils
import org.apache.hadoop.grumpy.tools.JobKiller
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

/**
 * This class 
 */
@Commons
class GrumpyJob extends Job {

  GrumpyJob() throws IOException {
  }

  GrumpyJob(String jobName) throws IOException{
    super(new Configuration(), jobName)
  }

  GrumpyJob(Configuration conf) throws IOException {
    super(conf)
  }

  GrumpyJob(Configuration conf, String jobName) throws IOException {
    super(conf, jobName)
  }

  void setupOutput(String outputURL) {
    log.info("Output directory is ${outputURL}")
    FileOutputFormat.setOutputPath(this, new Path(outputURL));
  }

  void addInput(String inputURL) {
    log.info("Input Path is ${inputURL}")
    FileInputFormat.addInputPath(this, new Path(inputURL));
  }


  void setupOutput(File output) {
    String outputURL = GrumpyUtils.convertToUrl(output)
    setupOutput(outputURL)
  }

  void addInput(File input) {
    String inputURL = GrumpyUtils.convertToUrl(input)
    addInput(inputURL)
  }

  void addJarList(List jarlist) {
    String listAsString = GrumpyUtils.joinList(jarlist, ",")
    configuration.set(Keys.JOB_KEY_JARS, listAsString)
  }

  void addNoJars() {
    this['tmpjars'] = ""
  }

  /**
   * Add the groovy jar. if this is groovy-all, you get everything.
   */
  String addGroovyJar() {
    return addJar(GString.class)
  }

  String addJar(Class jarClass) {
    String file = GrumpyUtils.findContainingJar(jarClass)
    if (!file) {
      throw new FileNotFoundException("No JAR containing class \"${jarClass}\"")
    }
    log.info("Jar containing class ${jarClass} is ${file}")
    addJar("file://" + file)
    file
  }

  void addJar(String jarFile) {
    append(Keys.JOB_KEY_JARS, jarFile)
  }

  public void append(String key, String value) {
    String attrlist = configuration.get(key, null)
    if (!attrlist) {
      attrlist = value;
    } else {
      attrlist += "," + value;
    }
    configuration.set(key, attrlist)
    attrlist
  }

  /**
   * if you want compressed output, ask for gzip
   * as it is the one that is there
   */
  def compressOutputToGzip() {
    FileOutputFormat.setCompressOutput(this, true)
    FileOutputFormat.setOutputCompressorClass(this, GzipCodec)
  }

  /**
   * Make the output sequenced block format
   */
  def outputSequencedBlocks() {
    conf.setOutputFormat(SequenceFileOutputFormat)
    SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK)
  }


  String get(String name) {
    conf.get(name)
  }

  void set(String name, Object value) {
    conf.set(name, value.toString())
  }

  def getAt(String name) {
    conf.get(name)
  }

  void putAt(String name, Object value) {
    set(name, value)
  }

  /**
   * Relay a property set to the configuration
   * @param name property name
   * @param value new value
   */
/*    def propertyMissing(String name, Object value) { set(name, value) }*/

  /**
   * Relay a property query to the configuration
   * @param name
   * @return the value or null
   */
/*
    String propertyMissing(String name) { get(name) }
*/

  /**
   * Apply a map of option pairs to the settings
   * @param options options map, can be null
   */
  void applyOptions(Map options) {
    log.debug("Setting options $options")
    if (options) {
      options.each { elt ->
        String key = elt.key.toString()
        String val = elt.value.toString()
        log.debug("settings $key=$val")
        set(key, val)
      }
    }
  }
  
  void applyOptionList(List options) {
    if(options) options.each { tuple ->
      def (k, v) = tuple
      set (k.toString(), v.toString())
    }
  }

  /**
   * Try to kill a job
   * @return true iff the operation was successful
   */
  boolean kill() {
    try {
      killJob()
      return true;
    } catch (Exception e) {
      log.debug("Failed to kill job: " + e, e)
      return false
    }
  }

  /**
   * Hit the switch to say "CSV output", use the extended text output format for this
   */
  void outputCSVFiles() {
    set(ExtensionOptions.KEY_EXTENSION, ".csv")
    set(ExtensionOptions.KEY_SEPARATOR, ",")
//    set(OUTPUT_FORMAT_CLASS_ATTR, NewAPIExtTextOutputFormat.name)
  }

  /**
   * Submit the job and wait for it to finish
   * @param verbose
   * @param terminateOnClientKill
   * @return
   */
  boolean submitAndWait(boolean verbose, boolean terminateOnClientKill) {
    submit()
    //get the job ID
    JobKiller terminator = JobKiller.targetForTermination(this)
    try {
      return waitForCompletion(verbose)
    } finally {
      terminator.unregister()
    }
  }
  /**
   * Create a basic job with the given M & R classes. 
   * The Groovy JAR is added as another needed JAR; the mapClass is set as the main jar of the job
   * @param name job name
   * @param conf configuration
   * @param mapClass mapper
   * @param reduceClass reducer
   * @return a job
   */
  static GrumpyJob createBasicJob(String name,
                                  JobConf conf,
                                  Class mapClass,
                                  Class reduceClass) {

    GrumpyJob job = new GrumpyJob(conf, name)

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
