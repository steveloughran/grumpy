/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
