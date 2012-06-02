package org.apache.hadoop.grumpy.projects.bluemine.mr

import groovy.util.logging.Commons
import org.apache.hadoop.grumpy.projects.bluemine.events.BlueEvent
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

@Commons
class DeviceNameMap extends MapToDevice {
  Text text = new Text()



  @Override
  protected void setup(final Mapper.Context context) {
    super.setup(context)
    //parser.parseDatestamp = false
  }

  /**
   * When invoked , event is the current event, outputKey is set to the Text to write
   * @param key
   * @param context
   */
  void process(LongWritable key, Mapper.Context context, BlueEvent event) {
    if (event.name != null) {
      if (!event.name.isEmpty()) {
        context.write(outputKey, text)
      }
    } else {
      log.info("Null name at line $key [$inputLine] in event $event")
    }
  }

}
