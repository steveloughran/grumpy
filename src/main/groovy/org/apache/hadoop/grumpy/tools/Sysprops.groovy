package org.apache.hadoop.grumpy.tools

/**
 * A class that provides array access to system properties
 */
final class Sysprops {

  static getAt(def k) {
    System.getProperty(k)
  }

  static setAt(def k, def v) {
    System.setProperty(k, v)
  }

  static mandatoryProperty(def k) {
    def v = getAt(k)
    assert v!=null
  }
  
  static getSystemPropertyList() {
    System.getProperties().collect { entry ->
     [entry.key, entry.value]
    }
  }
  
}
