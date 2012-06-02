package org.apache.hadoop.grumpy.projects.bluemine.reducers

import org.apache.hadoop.grumpy.projects.bluemine.events.BlueEvent
import org.apache.hadoop.grumpy.projects.bluemine.events.EventParser
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

/**
 * This emits an event as serialised events
 */
class EventCSVEmitReducer extends Reducer<Text, BlueEvent, NullWritable, Text> {

  private static final NullWritable NULL = NullWritable.get()
  private EventParser parser = new EventParser()
  private Text out = new Text()

  void reduce(Text key,
              Iterable<BlueEvent> values,
              Reducer.Context context) {
    values.each { event ->
      String csv = parser.convertToCSV(event, ',')
      out.set(csv)
      context.write(NULL, out)
    }
  }
}
