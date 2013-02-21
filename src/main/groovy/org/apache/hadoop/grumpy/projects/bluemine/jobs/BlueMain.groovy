package org.apache.hadoop.grumpy.projects.bluemine.jobs

import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.grumpy.ExitCodeException
import org.apache.hadoop.grumpy.projects.bluemine.BluemineOptions
import org.apache.hadoop.grumpy.tools.GrumpyUtils
import org.apache.hadoop.grumpy.tools.JobKiller
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
abstract class BlueMain {

  protected abstract boolean execute(String[] args);

  String getName() {"BlueMain"}

  protected static executeAndExit(BlueMain main, String[] args) {
    try {
      boolean success = main.execute(args)
      System.exit((success ? 0 : -1))
    } catch (ExitCodeException e) {
      log.error("On arguments $args")
      log.error(e.toString())
      System.exit(e.exitCode)
    } catch (Exception e) {
      log.error("On arguments $args")
      log.error(e.toString(), e)
      System.exit(-2)
    }
  }

  protected OptionAccessor parseCommandLine(String[] args) {
    CliBuilder cli = new CliBuilder(usage: "${name} [-j jobtracker] [-fs filesystem] [-v] [-D define=value] [-pf  properties] -s sourcedir -o outdir  ")
    // Create the list of options.

    cli.with {
      'do' longOpt: 'deloutdir', args: 1, argName: 'do', 'output directory -delete first'
      'D' longOpt: 'define', args: 2, valueSeparator: '=', argName: 'define', 'Definition'
      f longOpt: 'filesystem', args: 1, argName: 'hdfs', 'filesystem URL'
      h longOpt: 'help', 'Show usage information'
      j longOpt: 'jobtracker', args: 1, argName: 'tracker', 'URL of Job Tracker'
      o longOpt: 'outdir', args: 1, argName: 'out', 'directory for destination files'
      'ou' longOpt: 'outurl', args: 1, argName: 'out', 'URL for destination files'
      of longOpt: 'opfiles', args: 1, argName: 'optional properties', 'Property file'
      pf longOpt: 'pfile', args: 1, argName: 'properties', 'Property file'
      r longOpt: 'rsource', args: 1, argName: 'rsource', 'remote source URL'
      s longOpt: 'source', args: 1, argName: 'src', 'source file/directory'
      t longOpt: 'terminate', args: 1, argName: 'terminate', 'terminate job if client program is interrupted '
      v longOpt: 'verbose', 'verbose job output'
    }

    OptionAccessor options = cli.parse(args)
    if (!options) {
      options = null;
    }

    // Show usage text when -h or --help option is used.
    if (options.h) {
      cli.usage()
      options = null;
    }
    return options
  }




  protected static File outputDir(String name, boolean delete) {
    File dir = new File(name)
    if (dir.exists()) {
      if (delete) {
        FileUtil.fullyDelete(dir)
      } else {
        throw new ExitCodeException("Output directory exists and deletion not enabled")
      }
    }
    dir
  }

  public void applyDefinitions(OptionAccessor options, Configuration conf) {
    if (!options.Ds) {
      return
    }
    List definitions = options.Ds
    //convert into a list of key-val pairs by caching each value first
    String key
    definitions.eachWithIndex { entry, index ->
      if ((index & 1) == 0) {
        key = entry.toString()
      } else {
        String value = entry.toString()
        log.info("$key -> $value")
        conf.set(key, value)
      }
    }
  }

  /**
   * extract source and dest dirs from the options, set them on the job,
   * then execute it 
   * @param options
   * @param job
   * @return whether or not the job completed successfully
   */
  protected boolean bindAndExecute(OptionAccessor options, BluemineJob job) {
    boolean verbose = options.v
    bindSourceDir(options, job)
    bindOutputDir(options, job)
    job["mapred.submit.replication"] = 1
    job["mapred.map.tasks"] = 4
    log.info(job.toString())
    job.submit()
    //get the job ID
    String jobId = job.jobID;
    JobKiller terminator = JobKiller.targetForTermination(job)
    try {
      boolean success = job.waitForCompletion(verbose)
      return success
    } finally {
      terminator.unregister()
    }
  }

  /**
   * Bind up the output direcory -this may be cleared on startup if requested
   * @param options output directory
   * @param job job 
   */
  protected bindOutputDir(OptionAccessor options, BluemineJob job) {
    File outDir
    if (options.o) {
      outDir = outputDir(options.o, false)
      job.setupOutput(outDir)
    } else if (options."do") {
      outDir = outputDir(options."do", true)
      job.setupOutput(outDir)
    } else if (options."ou") {
      job.setupOutput(options."ou".toString())
    } else {
      throw new ExitCodeException("No output directory")
    }
  }

  /**
   * Bind up the source direcory -if not set fail
   * @param options output directory
   * @param job job 
   */
  protected bindSourceDir(OptionAccessor options, BluemineJob job) {
    def source = options.s
    if (source) {
      File srcDir
      log.debug("Unresolved --source is $source")
      srcDir = GrumpyUtils.requiredFile(source, "Job source ")
      log.info("Source URL is $srcDir")
      job.addInput(srcDir)
    } else {
      source = options.r
      if (!source) {
        throw new ExitCodeException("No source set with -s or -r")
      }
      log.info("Source URL is $source")
      job.addInput((String)source)
    }
  }

  /**
   * Load a property file, which must exist
   * @param conf configuration to load into
   * @param propertyFilename filename
   */
  protected void loadPropertyFile(JobConf conf, String propertyFilename, boolean required) {
    if (propertyFilename) {
      File propFile = new File(propertyFilename)
      if (required) GrumpyUtils.requiredFile(propertyFilename, "Property file")
      log.info("Loading property file $propFile")
      Properties props = new Properties()
      props.load(new FileInputStream(propFile))
      props.each { name, value ->
        conf.set(name.toString(), value.toString())
      }
    }
  }

  protected void loadPropertyFiles(JobConf conf, OptionAccessor options) {
    def propfiles = options.'pfs'
    if (propfiles) propfiles.each() {
      loadPropertyFile(conf, it, true)
    }
    propfiles = options.'ofs'
    if (propfiles) propfiles.each() {
      loadPropertyFile(conf, it, false)
    }
  }

  protected void loadProperties(JobConf conf, OptionAccessor options) {
    applyDefinitions(options, conf)
    loadPropertyFiles(conf, options)
  }

  protected void setFilesystemURL(JobConf conf, OptionAccessor options) {
    String hdfsURL = options.f ?: BluemineOptions.DEFAULT_FS;
    log.info("fs.default.name = $hdfsURL")
    conf.set("fs.default.name", hdfsURL)
  }

  protected void setTrackerURL(JobConf conf, OptionAccessor options) {
    String jtURL = options.j ?: BluemineOptions.DEFAULT_JOB_TRACKER;
    log.info("Job tracker URL = $jtURL")
    GrumpyUtils.checkURL("Job tracker", jtURL, 15000)
    conf.set("mapred.job.tracker", jtURL)
  }

}
