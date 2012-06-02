package org.apache.hadoop.grumpy.projects.bluemine.mr

import org.apache.hadoop.grumpy.projects.bluemine.events.BlueEvent
import org.apache.hadoop.mapreduce.Mapper

class MapToDevice extends AbstractBlueMapper {

  @Override
  String selectOutputKey(BlueEvent event, Mapper.Context context) {
    return event.device
  }

  static Class keyClass() { String }

}
