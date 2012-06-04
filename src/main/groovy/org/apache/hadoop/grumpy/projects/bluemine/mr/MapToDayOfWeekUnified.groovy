/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.hadoop.grumpy.projects.bluemine.mr

import org.apache.hadoop.grumpy.projects.bluemine.BluemineOptions as O

import groovy.util.logging.Commons
import org.apache.hadoop.grumpy.projects.bluemine.events.BlueEvent
import org.apache.hadoop.grumpy.projects.bluemine.events.EventParser
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

/**
 * Map to a day of the week. This mapper supports an offset day, defined by
 * {@link org.apache.hadoop.grumpy.projects.bluemine.BluemineOptions#OPT_HOUR_DAY_STARTS}; this allows the day to begin
 * at, say 3cd for 3 am; all events before that are deemed to belong to the previous day.
 *
 */
@Commons
class MapToDayOfWeekUnified extends Mapper<LongWritable, Text, Text, BlueEvent> {

  int startHour;

  IntWritable iw = new IntWritable(1)

  @Override
  protected void setup(Mapper.Context context) {
    super.setup(context)
    parser.parseDatestamp = true
    startHour = context.configuration.getInt(O.OPT_HOUR_DAY_STARTS, 0)
  }

  /**
   * convert to the previous day of the week. 6->5, 0 -> 7, etc.
   * @param weekday
   * @return the new value
   */
  int previousDayOfWeek(int weekday) {
    (weekday + 6) % 7
  }

  public static final String BLUEMINE_COUNTERS = "bluemine"
  protected EventParser parser = new EventParser()
  protected Text outputKey = new Text()
  protected String keyString
  private BlueEvent blueEvent = new BlueEvent()


  /**
   * Parse and emit events
   * @param key line #
   * @param value raw line
   * @param context ctx
   */
  protected void map(LongWritable key, Text value, Mapper.Context context) {
    parser.parse(blueEvent, value, "at offset " + key)
    Date date = blueEvent.datestamp
    int day = date.day
    if (date.hours < startHour) day = previousDayOfWeek(day)
    outputKey.set(day.toString())
    context.write(outputKey, iw)
  }


}
