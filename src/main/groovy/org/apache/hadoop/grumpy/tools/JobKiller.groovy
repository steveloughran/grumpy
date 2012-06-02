package org.apache.hadoop.grumpy.tools

import org.apache.hadoop.grumpy.GrumpyJob

/**
 * Registers a shutdown hook
 */
class JobKiller extends Thread {

    private GrumpyJob target

    

    @Override
    public void run() {
        GrumpyJob job
        // ensure that only once does this get run
        synchronized (this) {
            job = target
            target = null;
        }
        if (job != null) {
            job.kill()
        }
    }

    public synchronized void unregister() {
        if (target != null) {
            target = null;
            Runtime.runtime.removeShutdownHook(this)
        }
    }

    private synchronized void bind(GrumpyJob job) {
        if (target != null) {
            throw new IllegalStateException("Already bound to a target job : " + target)
        }
        target = job;
        Runtime.runtime.addShutdownHook(this);
    }


    static JobKiller targetForTermination(GrumpyJob job) {
        JobKiller killer = new JobKiller()
        killer.bind(job)
        killer;
    }

}
