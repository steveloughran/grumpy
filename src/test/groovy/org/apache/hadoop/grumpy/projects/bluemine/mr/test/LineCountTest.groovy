package org.apache.hadoop.grumpy.projects.bluemine.mr.test

import org.apache.hadoop.grumpy.GrumpyJob
import org.apache.hadoop.grumpy.projects.bluemine.mr.LineCountMapper
import org.apache.hadoop.grumpy.projects.bluemine.mr.testtools.BluemineTestBase
import org.apache.hadoop.grumpy.projects.bluemine.reducers.GroovyValueCountReducer

/**
 *
 */
class LineCountTest extends BluemineTestBase {


  void testLineCount() {
    GrumpyJob job
    File outDir
    (job, outDir) = createMRJob("linecount",
                                LineCountMapper,
                                GroovyValueCountReducer)
    runJob(job)
    dumpDir(LOG, outDir)
  }


}
