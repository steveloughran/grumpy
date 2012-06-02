package org.apache.hadoop.grumpy.projects.bluemine.reducers

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

/**
 * Reduce int count to more ints; very good for intermediate merges too.
 */
class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

  IntWritable iw = new IntWritable()

  void reduce(Text key,
              Iterable<IntWritable> values,
              Reducer.Context context) {
    int sum = (int) (values.collect() {it.get() }.sum())
    iw.set(sum)
    context.write(key, iw);
  }

}
