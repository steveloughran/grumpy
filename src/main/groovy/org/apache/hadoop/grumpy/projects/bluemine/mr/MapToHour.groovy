package org.apache.hadoop.grumpy.projects.bluemine.mr

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.grumpy.projects.bluemine.events.BlueEvent

class MapToHour extends MapToDevice {

    IntWritable iw = new IntWritable(1)

    /**
     * When invoked , event is the current event, outputKey is set to the Text to write
     * @param key
     * @param context
     */
    void process(LongWritable key, Mapper.Context context, BlueEvent event) {
        context.write(outputKey, iw)
    }

    @Override
    protected void setup(final Mapper.Context context) {
        super.setup(context)
        parser.parseDatestamp = true
    }

    @Override
    String selectOutputKey(BlueEvent event, Mapper.Context context) {
        Date date = event.datestamp
        return date.hours.toString()
    }


}
