

package java.org.apache.hadoop.grumpy

import org.apache.hadoop.mapred.JobConf

class GrumpyConf extends JobConf {

    def putAt(Object key, Object value) {
        set(key.toString(), value.toString())
        value
    }
    
    def getAt(Object key) {
        get(key.toString())
    }
    
}
