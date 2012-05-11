package java.org.apache.hadoop.grumpy

import groovy.transform.InheritConstructors
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster

@InheritConstructors
class GrumpyDFSCluster extends MiniDFSCluster {

    public static final String TEST_BUILD_DATA = "test.build.data"

    /**
     * fix the problem with MiniDFS clusters being system property driven
     * @param conf config file
     */
    GrumpyDFSCluster newInstance(Configuration conf,
                                 int numDataNodes) {
        Sysprops[TEST_BUILD_DATA] = conf[TEST_BUILD_DATA]
        new GrumpyDFSCluster(conf, numDataNodes)
    }


}
