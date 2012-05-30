package org.apache.hadoop.grumpy

import groovy.transform.InheritConstructors
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.MiniMRCluster

@InheritConstructors
class GrumpyMRCluster extends MiniMRCluster implements Closeable {

  public static final String HADOOP_LOG_DIR = "hadoop.log.dir"

  @Override
  void close() {
    shutdown()
  }

  static GrumpyMRCluster createInstance(int numTaskTrackers,
                                        String fsURI,
                                        int numDir,
                                        String[] hosts,
                                        JobConf conf) {
    Sysprops[HADOOP_LOG_DIR] =
      conf[HADOOP_LOG_DIR] ?: Sysprops["java.io.tmpdir"];
    new GrumpyMRCluster(numTaskTrackers, fsURI, numDir, hosts, conf)
  }

}
