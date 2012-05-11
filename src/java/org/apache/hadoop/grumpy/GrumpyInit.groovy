package java.org.apache.hadoop.grumpy

import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

/**
 * This contains the init logic that sets up various Hadoop classes to be
 * a bit more groovy
 */
@Commons
class GrumpyInit {

    static {
        /** array assignment for job confs */
        Configuration.metaClass.setAt = { k, v ->
            set(k.toString(), v.toString())
        }

        Configuration.metaClass.getAt = { k ->
            get(k)
        }

        /**
         * Job array assignments propagate to the inner config
         */
        Job.metaClass.setAt = { k, v ->
            configuration[k] = v
        }

        Job.metaClass.getAt = { k ->
            configuration(k)
        }

        log.debug("Grumpy is now initialized")

    }

}


