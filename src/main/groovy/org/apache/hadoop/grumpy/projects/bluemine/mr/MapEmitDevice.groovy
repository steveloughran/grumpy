package org.apache.hadoop.grumpy.projects.bluemine.mr

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.grumpy.projects.bluemine.events.BlueEvent

/**
 * a mapper that emits a device iff the emitCriteria is met
 */
class MapEmitDevice extends MapToDevice {


    @Override
    void process(LongWritable key, Mapper.Context context, BlueEvent event) {
        if(shouldEmit(event)) {
            context.write(outputKey, event)
        }
    }

    static Class keyClass() { Text }

    static Class valueClass() { BlueEvent }

    /**
     * Override point
     * @param event
     * @return
     */
    protected boolean shouldEmit(BlueEvent event) {
        true
    }
}
