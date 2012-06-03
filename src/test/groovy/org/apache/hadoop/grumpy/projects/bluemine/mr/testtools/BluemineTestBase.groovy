package org.apache.hadoop.grumpy.projects.bluemine.mr.testtools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.grumpy.GrumpyJob
import org.apache.hadoop.grumpy.projects.bluemine.BluemineOptions
import org.apache.hadoop.grumpy.projects.bluemine.jobs.BluemineJob
import org.apache.hadoop.grumpy.projects.bluemine.output.ExtTextOutputFormat
import org.apache.hadoop.grumpy.projects.bluemine.reducers.CountReducer
import org.apache.hadoop.grumpy.projects.bluemine.reducers.EventCSVEmitReducer
import org.apache.hadoop.grumpy.tools.GrumpyHadoopTestBase
import org.apache.hadoop.io.IntWritable

/**
 *
 */
class BluemineTestBase extends GrumpyHadoopTestBase implements BluemineOptions {

  public static final String TEST_INPUT_DATA_FILE = "test.input.data.file"
  public static final String GATE1_50K = "gate1-50k.csv"
  public static final String GATE1_SMALL = "gate1-small.csv"


  public static final String SMILEY = "gate1,2afaf990ce75f0a7208f7f012c8d12ad,,2006-10-30,16:06:54,Smiley"
  public static final String NO_NAME = "gate1,02e73779c77fcd4e9f90a193c4f3e7ff,,2006-10-30,16:07:15,"
  public static final String SMILEY2 = "gate1,2afaf990ce75f0a7208f7f012c8d12ad,,2006-10-30,16:07:24,Smiley"
  public static final String SMILEY3 = "gate1,2afaf990ce75f0a7208f7f012c8d12ad,,2006-10-30,16:07:54,Smiley"
  public static final String SMILEY4 = "gate1,2afaf990ce75f0a7208f7f012c8d12ad,,2006-10-30,16:08:56,Smiley"

  public static final String COMMA1 = "0017F2A49B6F,5c598739138321f92971dc6f6ec41344,,2006-10-30,21:34:11,,) Where am i?"
  public static final String COMMA2 = "0017F2A49B6F,5c598739138321f92971dc6f6ec41344,,2007-09-06,21:34:11,)\"\', Where am i?"
  public static final String VKLAPTOP = "gate3,f1191b79236083ce59981e049d863604,,2006-1-1,23:06:57,vklaptop"
  public static final String[] LINES = [
      NO_NAME,
      SMILEY,
      VKLAPTOP,
      COMMA1
  ]

  /**
   * These are erroneous records that show up in the real dataset
   */
  protected static final String[] BAD_LINES = [
      ",45c015c602e28f3f790e2937ff7a8a0b,,2009-01-21,09:14,",
      ",,"
  ]
  /**
   * Add the small gate1 input set to a job as the input
   * @param job job to patch
   */
  void addTestDataset(GrumpyJob job) {
    addDataset(job, GATE1_50K)
  }

  protected void addDataset(GrumpyJob job, String dataset) {
    String sourceFile = System.getProperty(TEST_INPUT_DATA_FILE, dataset);
    File file = getDataFile(sourceFile)
    addInput(job, file)
  }

  @Override
  GrumpyJob createBasicJob(String name,
                           Configuration conf,
                           Class mapClass,
                           Class reduceClass) {
    BluemineJob job = new BluemineJob(conf, name)
    job.jarByClass = mapClass
    job.mapperClass = mapClass
    job.reducerClass = reduceClass
    return job
  }

  /**
   * Create an initial MR job 
   * @param testname name of the test (which defines the output directoyr too
   * @param mapClass class to use in map
   * @param reduceClass class to use in reduction
   * @return ( job : GrumpyJob , output directory : file )
   */
  List createMRJob(Map options, String testname, Class mapper, Class reduceClass) {
    BluemineJob job
    File outDir
    (job, outDir) = createMRJobNoDataset(testname, mapper, reduceClass)
    addTestDataset(job)
    job.applyOptions(options)
    [job, outDir]
  }

  List createMRJob(String testname, Class mapClass, Class reduceClass) {

    createMRJob(null,
                testname,
                mapClass,
                reduceClass)
  }

  protected List createMRJobNoDataset(String testname, Class mapClass, Class reduceClass) {
    createMRJobNoDataset(null,
                         testname,
                         mapClass,
                         reduceClass)
  }

  protected List createMRJobNoDataset(Map options, String testname, Class mapClass, Class reduceClass) {
    Configuration conf = createJobConfiguration()
    BluemineJob job = (BluemineJob) createTextKeyIntValueJob(testname,
                                                             conf,
                                                             mapClass,
                                                             reduceClass)
    File outDir = addTestOutputDir(job, testname)
    job.applyOptions(options)
    [job, outDir]
  }

  /**
   * Run an event job against the specified mapper, using int
   * as the output value of the map, and the Count reducer as the reducer
   * @param name job name
   * @param mapper mapper class
   * @return the output directory
   *
   */
  File runCountJob(String name, Class mapper) {
    runCountJob(null, name, mapper)
  }

  /**
   * Run a job
   * @param options map of options, can be named parameters
   * @param name job name
   * @param mapper mapper
   * @return the output directory after the run
   */
  File runCountJob(Map options, String name, Class mapper) {
    GrumpyJob job
    File outDir
    (job, outDir) = createMRJob(options,
                                name,
                                mapper,
                                CountReducer)
    job.mapOutputValueClass = IntWritable
    job.outputFormatClass = ExtTextOutputFormat
    runJob(job)
    dumpDir(LOG, outDir)
    outDir
  }

  protected void makeMapEmitEvents(BluemineJob job) {
    job.makeMapEmitEvents()
  }

  /**
   * Run a job
   * @param options map of options, can be named parameters
   * @param name job name
   * @param mapper mapper
   * @return the output directory after the run
   */
  def runEventCSVJob(Map options, String name, Class mapper, List optionList) {
    BluemineJob job
    File outDir
    (job, outDir) = createMRJob(options,
                                name,
                                mapper,
                                EventCSVEmitReducer)
    job.applyOptionList(optionList)
    makeMapEmitEvents(job)
    log.info("Executing job: $job")
    boolean success = job.waitForCompletion(true)
    dumpDir(LOG, outDir)
    [outDir, success]
  }
}
