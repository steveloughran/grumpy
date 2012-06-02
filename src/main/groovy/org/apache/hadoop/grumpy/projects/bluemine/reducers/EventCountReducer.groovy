package org.apache.hadoop.grumpy.projects.bluemine.reducers

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.grumpy.projects.bluemine.events.BlueEvent

class EventCountReducer extends Reducer<Text, BlueEvent, Text, IntWritable> {

    IntWritable iw = new IntWritable()

    void reduce(Text key,
                Iterable<BlueEvent> values,
                Reducer.Context context) {
        int sum = (int) (values.collect() {event -> 1 }.sum())
        iw.set(sum)
        context.write(key, iw);
    }

}
