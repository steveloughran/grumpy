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

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.grumpy.GrumpyJob
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.MiniMRCluster

/**
 * This is a groovy test base for Hadoop MR jobs
 */
abstract class GrumpyHadoopTestBase extends GroovyTestCase
implements Closeable {
  protected static Log LOG = LogFactory.getLog(this);

  /**
   * Test property used to define the input directory of data:
   * {@value}
   */

  public static final String TEST_INPUT_DATA_DIR = "test.input.data.dir"

  /**
   * Test property used to define the output directory of data:
   * {@value}
   */

  public static final String TEST_OUTPUT_DATA_DIR = "test.output.data.dir"
  protected MiniMRCluster mrCluster
  protected MiniDFSCluster dfsCluster

  void createMrCluster(int nodes, String fsURI, JobConf conf) {
    mrCluster = MiniClusters.createMiniMRCluster(nodes, fsURI, 1, conf)
  }

  void createDfsCluster(int nodes, Properties properties) {
    JobConf conf = createClusterJobConf()
    conf.add(properties);
    dfsCluster = MiniClusters.createMiniDFSCluster(conf, 0);

  }

  JobConf createClusterJobConf() {
    return new JobConf();
  }

  @Override
  protected void tearDown() {
    close()
    super.tearDown()
  }

  @Override
  void close() {
    mrCluster?.shutdown()
    mrCluster = null
    dfsCluster?.shutdown()
    dfsCluster = null
  }


  Configuration createJobConfiguration() {
    Configuration conf = new Configuration();
    return conf
  }

  GrumpyJob createTextKeyIntValueJob(String name,
                                     Configuration conf,
                                     Class mapClass,
                                     Class reduceClass) {
    GrumpyJob job = createBasicJob(name,
                                   conf,
                                   mapClass,
                                   reduceClass)
    job.mapOutputKeyClass = Text.class
    job.mapOutputValueClass = IntWritable.class
    job
  }

  GrumpyJob createBasicJob(String name,
                           Configuration conf,
                           Class mapClass,
                           Class reduceClass) {
    GrumpyJob job = new GrumpyJob(conf, name)
    job.jarByClass = mapClass
    job.mapperClass = mapClass
    job.reducerClass = reduceClass
    job
  }

  void setupOutput(GrumpyJob job, String outputURL) {
    job.setupOutput(outputURL)
  }

  void addInput(GrumpyJob job, String inputURL) {
    job.addInput(inputURL)
  }

  void setupOutput(GrumpyJob job, File output) {
    job.setupOutput(output)
  }

  void addInput(GrumpyJob job, File input) {
    job.addInput(input)
  }

  File getTestDataDir() {
    File dataDirectory = getSyspropFile(TEST_INPUT_DATA_DIR,Sysprops['user.dir']+'/data/small/')
    if (!dataDirectory.exists()) {
      throw new IOException("Property ${TEST_INPUT_DATA_DIR} is set to a nonexistent directory ${dataDirectory}")
    }
    if (!dataDirectory.directory) {
      throw new IOException("Property ${TEST_INPUT_DATA_DIR} is not a directory: ${dataDirectory}")
    }
    return dataDirectory;
  }

  /**
   * Get the filename from a specific property file
   * @param propertyName mandatory property name
   * @return the file referred to (may be relative)
   * @throws IOException if the property is unset
   */
  protected File getSyspropFile(String propertyName) throws IOException {
    getSyspropFile propertyName, null
  }

  protected File getSyspropFile(String propertyName, String defVal) throws IOException {
    String dataDir = Sysprops[propertyName] ?: defVal
    if (!dataDir) {
      throw new IOException("Unset property: ${propertyName} in ${Sysprops.systemPropertyList}");
    }
    File dataDirectory = new File(dataDir)
    return dataDirectory
  }

  /**
   * Set up the output dir for tests
   * @param testDir the test directory under the directory set by
   * the property {@link #TEST_OUTPUT_DATA_DIR}
   * @return the output directory for the job
   * @throws IOException if the property is unset
   */
  File prepareTestOutputDir(GrumpyJob job, String testDir) throws IOException {
    File outDir = getSyspropFile(TEST_OUTPUT_DATA_DIR, Sysprops['user.dir']+"/target/surefire/out")
    log.info("${TEST_OUTPUT_DATA_DIR} = ${outDir}")
    File jobOutDir = new File(outDir, testDir);
    GrumpyUtils.deleteDirectoryTree(jobOutDir)
    return jobOutDir
  }

  String convertToUrl(File file) {
    return file.toURI().toString();
  }

  File getDataFile(String filename) {
    File dataDir = testDataDir
    File testData = new File(dataDir, filename)
    if (!testData.exists()) {
      throw new IOException("Missing file ${testData}")
    }
    return testData
  }

  File addTestOutputDir(GrumpyJob job, String subdir) {
    File dir = prepareTestOutputDir(job, subdir)
    setupOutput(job, dir)
    return dir
  }


  void runJob(GrumpyJob job) {
    log.info("Executing job: $job")
    boolean success = job.waitForCompletion(true)
    assertTrue("Job failed", success)
  }

  long dumpDir(Log dumpLog, File dir) {
    GrumpyUtils.dumpDir(dumpLog, dir, "part-")
  }
}
