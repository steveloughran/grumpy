package org.apache.hadoop.grumpy

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.conf.Configuration

/**
 * This is a set of static helper methods to work on configurations, 
 * available with the <code>use</code> method.
 * 
 * The only one that adds anything new to the normal use is the 
 * addConfigMap() operation; the others are only there to simplify
 * metaprogramming operations.
 */
class GrumpyConfigHelper {

  static def setConfigEntry(Configuration self, def key, def value) {
    self.set(key.toString(), value.toString())
  }

  static String getConfigEntry(Configuration self, def key) {
    self.get(key.toString())
  }

  /**
   * Set an entire map full of values
   * @param map map
   * @return nothing
   */
  static def addConfigMap(Configuration self,Map map) {
    map.every { mapEntry ->
      setConfigEntry(self, mapEntry.key, mapEntry.value)  
    }
  }
}
