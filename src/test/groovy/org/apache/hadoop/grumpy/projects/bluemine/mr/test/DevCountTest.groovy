package org.apache.hadoop.grumpy.projects.bluemine.mr.test

import org.apache.hadoop.grumpy.projects.bluemine.mr.DeviceCountMap
import org.apache.hadoop.grumpy.projects.bluemine.mr.testtools.BluemineTestBase

class DevCountTest extends BluemineTestBase {


    void testDevCount() {
        runCountJob("devcount", DeviceCountMap)
    }

}
