package org.apache.hadoop.grumpy.projects.bluemine.mr

import org.apache.hadoop.grumpy.projects.bluemine.events.BlueEvent
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Mapper

class MapToFellowTraveller extends AbstractBlueMapper {

  private long default_duration = 30000;

  Set<BlueEvent> activeEvents = new TreeSet<BlueEvent>()

  @Override
  void process(LongWritable key, Mapper.Context context, BlueEvent event) {
    //event is the curret event.
    Date started = event.datestamp
    long startedMs = started.time;
    long startedMsLessTimeout = started.time - default_duration;
    long duration = default_duration;
    boolean isAlreadyActive = false;
    activeEvents.each { it ->
      if (it.device == event.device) {
        //same device, extend the original event time
        it.duration += started - it.datestamp.time + default_duration;
        isAlreadyActive = true;
      } else {
        //no match, maybe remove?
        if (it.endtime < startedMsLessTimeout) {
          activeEvents.remove(it);
        }
      }
    }
    if (!isAlreadyActive) {
      //not already active, so issue a new event for each of the overlapped devices that we hadn't encountered before.
      activeEvents.each() { it ->
        String outkey = event.device
        outputKey.set(outkey, context)
        context.write(outputKey, it)
      }

      //now add the event to the set
      event.duration = default_duration
      activeEvents.add(event)
    }

  }


  @Override
  String selectOutputKey(BlueEvent event, Mapper.Context context) {
    return event.device
  }


}
