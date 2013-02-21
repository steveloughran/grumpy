package org.apache.hadoop.grumpy.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.grumpy.Keys
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.hdfs.server.common.HdfsConstants
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.MiniMRCluster

class MiniClusters {

  public static final String HADOOP_LOG_DIR = "hadoop.log.dir"



  static MiniMRCluster createMiniMRCluster(int numTaskTrackers,
                                        String fsURI,
                                        int numDir,
                                        JobConf conf) throws IOException {
    Sysprops[HADOOP_LOG_DIR] =
      conf[HADOOP_LOG_DIR] ?: Sysprops["java.io.tmpdir"];
    new MiniMRCluster(numTaskTrackers, fsURI, numDir, null, null, conf)
  }


  /**
 * fix the problem with MiniDFS clusters being system property driven
 * @param conf config file
 */
  static MiniDFSCluster createMiniDFSCluster(Configuration conf,
                                       int numDataNodes) {
    Sysprops[Keys.TEST_DATA_DIR] = conf[Keys.TEST_DATA_DIR]
    new MiniDFSCluster(conf, numDataNodes, HdfsConstants.StartupOption.FORMAT)
  }
}
