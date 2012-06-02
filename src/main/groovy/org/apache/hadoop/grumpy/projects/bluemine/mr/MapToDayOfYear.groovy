package org.apache.hadoop.grumpy.projects.bluemine.mr

import org.apache.hadoop.grumpy.projects.bluemine.events.BlueEvent
import org.apache.hadoop.mapreduce.Mapper

/**
 * Map to a day of the year
 *
 */
class MapToDayOfYear extends MapToHour {

  final GregorianCalendar cal = new GregorianCalendar()

  @Override
  protected void setup(Mapper.Context context) {
    super.setup(context)
  }

  @Override
  String selectOutputKey(BlueEvent event, Mapper.Context context) {
    Date date = event.datestamp
    cal.time = date
    int dayOfYear = cal.get(GregorianCalendar.DAY_OF_YEAR)
    return dayOfYear
  }


}
