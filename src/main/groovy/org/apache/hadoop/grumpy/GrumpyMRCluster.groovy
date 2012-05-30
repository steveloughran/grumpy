package org.apache.hadoop.grumpy

import groovy.transform.InheritConstructors
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.MiniMRCluster

class GrumpyMRCluster extends MiniMRCluster implements Closeable {

  public static final String HADOOP_LOG_DIR = "hadoop.log.dir"

  GrumpyMRCluster(int numTaskTrackers, String namenode, int numDir, String[] racks, String[] hosts, JobConf conf) 
    throws IOException {
    super(numTaskTrackers, namenode, numDir, racks, hosts, conf)
  }

  @Override
  void close() {
    shutdown()
  }

  static GrumpyMRCluster createInstance(int numTaskTrackers,
                                        String fsURI,
                                        int numDir,
                                        JobConf conf) {
    Sysprops[HADOOP_LOG_DIR] =
      conf[HADOOP_LOG_DIR] ?: Sysprops["java.io.tmpdir"];
    new GrumpyMRCluster(numTaskTrackers, fsURI, numDir, null, null, conf)
  }

}
