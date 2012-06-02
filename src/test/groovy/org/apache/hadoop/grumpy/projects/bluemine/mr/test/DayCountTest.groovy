package org.apache.hadoop.grumpy.projects.bluemine.mr.test

import org.apache.hadoop.grumpy.projects.bluemine.mr.MapToDayOfWeek
import org.apache.hadoop.grumpy.projects.bluemine.mr.testtools.BluemineTestBase

class DayCountTest extends BluemineTestBase {

    void testHourCount() {
        runCountJob("daycount", MapToDayOfWeek)
    }

}
