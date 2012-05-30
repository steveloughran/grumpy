package org.apache.hadoop.grumpy

/**
 * A class that provides array access to system properties
 */
class Sysprops {

  static def getAt(def k) {
    System.getProperty(k)
  }

  static def setAt(def k, def v) {
    System.setProperty(k, v)
  }

}
