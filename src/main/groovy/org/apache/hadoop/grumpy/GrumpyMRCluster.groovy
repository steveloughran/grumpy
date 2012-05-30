package org.apache.hadoop.grumpy

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.MiniMRCluster

class GrumpyMRCluster  {

  public static final String HADOOP_LOG_DIR = "hadoop.log.dir"



  static MiniMRCluster createInstance(int numTaskTrackers,
                                        String fsURI,
                                        int numDir,
                                        JobConf conf) throws IOException {
    Sysprops[HADOOP_LOG_DIR] =
      conf[HADOOP_LOG_DIR] ?: Sysprops["java.io.tmpdir"];
    new MiniMRCluster(numTaskTrackers, fsURI, numDir, null, null, conf)
  }

}
