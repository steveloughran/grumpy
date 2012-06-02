package org.apache.hadoop.grumpy.projects.bluemine.mr

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.grumpy.projects.bluemine.events.EventParser

/**
 * Minimal parser for university lecture
 */
class SimpleDeviceMapper extends Mapper {
    def parser = new EventParser()
    def one = new IntWritable(1)


    def map(LongWritable k, Text v, Mapper.Context ctx) {
        def event = parser.parse(v)
        ctx.write(event.device, one)
    }
}