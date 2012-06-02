package org.apache.hadoop.grumpy.projects.bluemine.reducers

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.grumpy.projects.bluemine.events.BlueEvent

/**
 * This emits all events associated with a device in temporal order
 */
class EventEmitReducer extends Reducer<Text, BlueEvent, NullWritable, BlueEvent> {

    private static final NullWritable NULL = NullWritable.get()

    void reduce(Text key,
                Iterable<BlueEvent> values,
                Reducer.Context context) {

        //first sort all the values (the CPU killer
        TreeMap<Long, BlueEvent> tree = new TreeMap<Long, BlueEvent>()
        values.each { event ->
            tree.put(event.starttime, event)
        }
        tree.navigableKeySet().each { timestamp ->
            BlueEvent event = tree.get(timestamp)
            context.write(NULL, event)
        }
    }
}
