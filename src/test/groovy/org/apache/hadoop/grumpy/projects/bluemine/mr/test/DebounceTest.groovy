package org.apache.hadoop.grumpy.projects.bluemine.mr.test


import org.apache.hadoop.grumpy.projects.bluemine.jobs.BluemineJob
import org.apache.hadoop.grumpy.projects.bluemine.mr.DebounceMap
import org.apache.hadoop.grumpy.projects.bluemine.mr.testtools.BluemineTestBase
import org.apache.hadoop.grumpy.projects.bluemine.reducers.EventCSVEmitReducer

class DebounceTest extends BluemineTestBase {

  void testDebounceSmall() {
    BluemineJob job
    File outDir
    (job, outDir) = createMRJobNoDataset("debounce-small",
                                         DebounceMap,
                                         EventCSVEmitReducer)
    addDataset(job, GATE1_SMALL)
    makeMapEmitEvents(job)
    job.compressOutputToGzip()
    runJob(job)
    dumpDir(LOG, outDir)
    outDir
  }


  void testDebounceJob() {
    runEventCSVJob([:],
                   "debounce",
                   DebounceMap,
                   [])
  }
}
