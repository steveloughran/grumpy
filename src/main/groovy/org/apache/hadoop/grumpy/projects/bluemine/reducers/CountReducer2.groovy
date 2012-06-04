package org.apache.hadoop.grumpy.projects.bluemine.reducers

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

/**
 * Minimal reducer used in university lecture -inefficient
 */
class CountReducer2 extends Reducer {
  def reduce(Text k,
             Iterable values,
             Reducer.Context ctx) {
    def sum = values.collect() {it.get() }.sum()
    ctx.write(k, new IntWritable(sum));
  }
}
