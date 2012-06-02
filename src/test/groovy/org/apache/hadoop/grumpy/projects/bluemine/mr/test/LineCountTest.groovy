package org.apache.hadoop.grumpy.projects.bluemine.mr.test

import org.apache.hadoop.grumpy.projects.bluemine.mr.GroovyLineCountMapper
import org.apache.hadoop.grumpy.projects.bluemine.mr.testtools.BluemineTestBase
import org.apache.hadoop.grumpy.projects.bluemine.reducers.GroovyValueCountReducer
import org.apache.hadoop.grumpy.GrumpyJob

/**
 *
 */
class LineCountTest extends BluemineTestBase {


    void testLineCount() {
        GrumpyJob job
        File outDir
        (job, outDir) = createMRJob("linecount",
                                    GroovyLineCountMapper,
                                    GroovyValueCountReducer)
        runJob(job)
        dumpDir(LOG, outDir)
    }


}
