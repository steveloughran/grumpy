package org.apache.hadoop.grumpy.projects.bluemine.mr.test

import org.apache.hadoop.grumpy.projects.bluemine.mr.MapToHour
import org.apache.hadoop.grumpy.projects.bluemine.mr.testtools.BluemineTestBase

class HourCountTest extends BluemineTestBase {

  void testHourCount() {
    runCountJob("hourcount", MapToHour)
  }

}
