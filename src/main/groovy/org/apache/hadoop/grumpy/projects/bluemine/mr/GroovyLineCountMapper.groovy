package org.apache.hadoop.grumpy.projects.bluemine.mr

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

@SuppressWarnings("GroovyAssignabilityCheck")
class GroovyLineCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    static final def emitKey = new Text("lines")
    static final def one = new IntWritable(1)

    @Override
    void map(LongWritable key,
             Text value,
             Mapper.Context context) {
        context.write(emitKey, one)
    }
}
