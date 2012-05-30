package org.apache.hadoop.grumpy

import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

/**
 * This contains the init logic that sets up various Hadoop classes to be
 * a bit more groovy. You must instantiate a GrumpyInit class to force injection
 * into the system.
 */
@Commons
class Grumpy {

  static {
    /** array assignment for job confs */
    Configuration.metaClass.setAt = { k, v ->
      set(k.toString(), v.toString())
    }

    Configuration.metaClass.getAt = { k ->
      get(k)
    }

    /**
     * Add an entire map
     */
    Configuration.metaClass.add = {map ->
      map.each {mapEntry ->
        set((mapEntry.key).toString(), (mapEntry.value).toString() )
      }
    }

    /**
     * Job array assignments propagate to the inner config
     */
    Job.metaClass.setAt = { k, v ->
      configuration[k] = v
    }

    Job.metaClass.getAt = { k ->
      configuration[k]
    }

    /**
     * Add an entire map
     */
    Job.metaClass.add = {map -> configuration.add(map) }


  }

  Grumpy() {
    log.debug("welcome to grumpy");
  }
}


