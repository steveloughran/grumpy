package java.org.apache.hadoop.grumpy

import groovy.transform.InheritConstructors
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.MiniMRCluster

@InheritConstructors
class GrumpyMRCluster extends MiniMRCluster {

    public static final String HADOOP_LOG_DIR = "hadoop.log.dir"

    static GrumpyMRCluster createInstance(int numTaskTrackers,
                                          String fsURI,
                                          int numDir,
                                          String[] hosts,
                                          JobConf conf) {
        Sysprops[HADOOP_LOG_DIR] =
            conf.get(HADOOP_LOG_DIR, Sysprops["java.io.tmpdir"]);
        new GrumpyMRCluster(numTaskTrackers, fsURI, numDir, hosts, conf)
    }

}
