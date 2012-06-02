package org.apache.hadoop.grumpy.projects.bluemine.mr.test

import org.apache.hadoop.grumpy.projects.bluemine.mr.FilterDeviceByYear
import org.apache.hadoop.grumpy.projects.bluemine.mr.testtools.BluemineTestBase

class YearFilterTest extends BluemineTestBase {


    void testYear2006() {
        runEventCSVJob("bluemine.filter.year": 2006,
                "yearfilter",
                FilterDeviceByYear)
    }
    
    void testYear2007() {
        File outdir
        boolean success
        (outdir, success) = runEventCSVJob("bluemine.filter.year": 2007,
                "yearfilter2007",
                FilterDeviceByYear)
        long size = dumpDir(LOG, outdir)
        assert 0 == size
    }
    void testYear0() {
        File outdir
        boolean success
        (outdir, success) = runEventCSVJob(
                "bluemine.filter.year": 0,
                "yearfilter0",
                FilterDeviceByYear)
        assertFalse("Job should have failed", success)
    }

}
